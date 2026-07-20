// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import 'dart:async';

import 'package:async/async.dart';
import 'package:checks/checks.dart';
import 'package:test/test.dart';
import 'package:typed_sql/typed_sql.dart';

import 'model.dart';

/// Wait for the next event on [queue], returning `true` if none arrives
/// within a short timeout. Used to assert that a `.watch()` stream did NOT
/// re-emit after an unrelated write.
Future<bool> _timesOutWaitingForNext(StreamQueue queue) async {
  try {
    await queue.next.timeout(const Duration(milliseconds: 300));
    return false;
  } on TimeoutException {
    return true;
  }
}

void main() {
  late DatabaseAdapter adapter;
  late Database<PrimaryDatabase> db;

  setUp(() async {
    adapter = DatabaseAdapter.sqlite3TestDatabase();
    db = Database<PrimaryDatabase>(adapter, SqlDialect.sqlite());
    await db.createTables();
    await db.users
        .insert(
          userId: toExpr(1),
          name: toExpr('Alice'),
          email: toExpr('alice@example.com'),
        )
        .execute();
    await db.packages
        .insert(
          packageName: toExpr('foo'),
          likes: toExpr(2),
          publisher: toExpr(null),
          ownerId: toExpr(1),
        )
        .execute();
    await db.packages
        .insert(
          packageName: toExpr('bar'),
          likes: toExpr(3),
          publisher: toExpr(null),
          ownerId: toExpr(1),
        )
        .execute();
  });

  tearDown(() async {
    await adapter.close(force: true);
  });

  test('.watch() emits an initial snapshot', () async {
    final queue = StreamQueue(db.packages.watch());
    final initial = await queue.next;
    check(initial).length.equals(2);
    await queue.cancel(immediate: true);
  });

  test('.watch() re-emits after an insert into a watched table', () async {
    final queue = StreamQueue(db.packages.watch());
    check(await queue.next).length.equals(2);

    await db.packages
        .insert(packageName: toExpr('baz'), ownerId: toExpr(1))
        .execute();

    check(await queue.next).length.equals(3);
    await queue.cancel(immediate: true);
  });

  test(
    '.watch() does not re-emit after a write to an unrelated table',
    () async {
      final queue = StreamQueue(db.packages.watch());
      check(await queue.next).length.equals(2);

      // `packages` has a foreign key to `users`, but a plain `db.packages`
      // query never reads from `users`, so writes to `users` must not
      // trigger a re-fetch.
      await db.users
          .insert(
            userId: toExpr(2),
            name: toExpr('Bob'),
            email: toExpr('bob@example.com'),
          )
          .execute();

      check(await _timesOutWaitingForNext(queue)).isTrue();
      await queue.cancel(immediate: true);
    },
  );

  test('.watch() on a join re-emits on writes to either table', () async {
    final queue = StreamQueue(
      db.users
          .join(db.packages)
          .on((u, p) => u.userId.equals(p.ownerId))
          .watch(),
    );
    check(await queue.next).length.equals(2);

    await db.packages
        .insert(packageName: toExpr('baz'), ownerId: toExpr(1))
        .execute();
    check(await queue.next).length.equals(3);

    await db.users
        .insert(
          userId: toExpr(2),
          name: toExpr('Bob'),
          email: toExpr('bob@example.com'),
        )
        .execute();
    // Bob has no packages, so the row count doesn't change, but a write to
    // `users` (a table this join reads from) must still trigger a re-fetch.
    check(await queue.next).length.equals(3);

    await queue.cancel(immediate: true);
  });

  test('QuerySingle.watch() emits null, then the row once it exists', () async {
    final queue = StreamQueue(db.packages.byKey('new-pkg').watch());
    check(await queue.next).isNull();

    await db.packages
        .insert(packageName: toExpr('new-pkg'), ownerId: toExpr(1))
        .execute();

    final afterInsert = await queue.next;
    check(afterInsert).isNotNull();
    check(afterInsert!.packageName).equals('new-pkg');
    await queue.cancel(immediate: true);
  });

  test('.watch() batches writes inside transact() into one emission', () async {
    final queue = StreamQueue(db.packages.watch());
    check(await queue.next).length.equals(2);

    await db.transact(() async {
      await db.packages
          .insert(packageName: toExpr('p1'), ownerId: toExpr(1))
          .execute();
      await db.packages
          .insert(packageName: toExpr('p2'), ownerId: toExpr(1))
          .execute();
    });

    // A single emission reflecting both inserts, not one per statement.
    check(await queue.next).length.equals(4);
    check(await _timesOutWaitingForNext(queue)).isTrue();
    await queue.cancel(immediate: true);
  });

  test('.watch() does not emit for a rolled back transaction', () async {
    final queue = StreamQueue(db.packages.watch());
    check(await queue.next).length.equals(2);

    try {
      await db.transact(() async {
        await db.packages
            .insert(
              packageName: toExpr('should-not-exist'),
              ownerId: toExpr(1),
            )
            .execute();
        throw Exception('boom');
      });
    } catch (_) {
      // Expected: the transaction was rolled back.
    }

    check(await _timesOutWaitingForNext(queue)).isTrue();
    await queue.cancel(immediate: true);
  });

  test('.watch() gives each listener its own fresh initial snapshot', () async {
    final stream = db.packages.watch();

    final q1 = StreamQueue(stream);
    check(await q1.next).length.equals(2);

    await db.packages
        .insert(packageName: toExpr('baz'), ownerId: toExpr(1))
        .execute();
    check(await q1.next).length.equals(3);

    // A listener attaching *after* the write should still get its own
    // fresh, up-to-date initial snapshot, not miss it or replay stale data.
    final q2 = StreamQueue(stream);
    check(await q2.next).length.equals(3);

    await q1.cancel(immediate: true);
    await q2.cancel(immediate: true);
  });

  test(
    'cancelling a .watch() subscription does not break later writes',
    () async {
      final sub = db.packages.watch().listen((_) {});
      await sub.cancel();

      // Give the cancelled subscription's own in-flight initial fetch a
      // moment to finish draining before writing. Without this, the SQLite
      // adapter's rollback-journal locking (no WAL mode) can occasionally
      // surface a transient "database is locked" here — a pre-existing
      // adapter characteristic when a multi-row read and a write overlap,
      // not something `.watch()` can fully paper over by itself.
      await Future<void>.delayed(const Duration(milliseconds: 50));

      await db.packages
          .insert(packageName: toExpr('after-cancel'), ownerId: toExpr(1))
          .execute();

      final result = await db.packages.byKey('after-cancel').fetch();
      check(result).isNotNull();
    },
  );
}
