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

part of 'typed_sql.dart';

/// {@category transactions}
final class Database<T extends Schema> {
  Database(DatabaseAdapter adapter, SqlDialect dialect)
    : _adapter = adapter,
      _dialect = dialect;

  final SqlDialect _dialect;
  final DatabaseAdapter _adapter;

  late final _zoneKey = (this, #_transaction);
  late final _pendingChangesKey = (this, #_pendingChanges);

  Executor get _executor => Zone.current[_zoneKey] as Executor? ?? _adapter;

  /// Table names that have changed, but not yet been notified, because we're
  /// inside the [Zone] of an ongoing [transact] call.
  Set<String>? get _pendingChanges =>
      Zone.current[_pendingChangesKey] as Set<String>?;

  /// Broadcast of table names changed by writes made through this
  /// [Database]. Used internally to power `.watch()`.
  final _changes = StreamController<Set<String>>.broadcast();

  Stream<Set<String>> get _tableChanges => _changes.stream;

  /// Notify watchers that [affectedTables] may have changed.
  ///
  /// If called while inside a [transact] [Zone], the notification is merged
  /// into that transaction's pending set and only emitted once, when the
  /// outermost [transact] call commits.
  void _notifyChanged(Set<String> affectedTables) {
    if (affectedTables.isEmpty) {
      return;
    }
    final pending = _pendingChanges;
    if (pending != null) {
      pending.addAll(affectedTables);
    } else {
      _changes.add(affectedTables);
    }
  }

  /// Start a transaction an execute [fn] in a [Zone] where all operations on
  /// this [Database] happens in the transaction.
  ///
  /// When [fn] completes the transaction will be committed. If [fn] throws an
  /// [Exception] then the transaction will be rolled back, and the call to
  /// [transact] will throw an [TransactionAbortedException].
  ///
  /// Inside the transaction [Zone] result streams will not respect
  /// back-pressure. This means that all rows may be buffered in memory!
  /// Avoid scanning large result sets inside the transaction.
  ///
  /// Using [transact] inside a transaction will create `SAVEPOINT` in SQL.
  ///
  /// > [!WARNING]
  /// > All database operations inside [fn] **must be awaited**. When [fn]
  /// > returns the transaction will be committed or rolledback, further
  /// > operations inside the transaction [Zone] will throw!
  /// >
  /// > Avoid using [unawaited] and [scheduleMicrotask] inside [fn].
  Future<R> transact<R>(
    Future<R> Function() fn,
  ) async {
    // If we're already inside a transaction Zone (a SAVEPOINT), reuse the
    // outer pending-changes set so watchers are only notified once, when the
    // outermost transaction commits.
    final outerPending = _pendingChanges;
    final pending = outerPending ?? <String>{};

    final result = await _executor.transact((tx) async {
      return await runZoned(
        fn,
        zoneValues: {
          _zoneKey: tx,
          _pendingChangesKey: pending,
        },
      );
    });

    if (outerPending == null) {
      _notifyChanged(pending);
    }

    return result;
  }

  Stream<RowReader> _query(SqlTask task) => switch (task) {
    SingleSqlTask(:final sql, :final params) => _executor.query(sql, params),
    PipelinedSqlTask(:final sql, :final paramsList) => _executor.queryMany(
      sql,
      paramsList,
    ),
    ScriptSqlTask() => throw AssertionError(
      'Unreachable! TODO: Make this work!',
    ),
  };

  Future<void> _execute(SqlTask task, Set<String> affectedTables) async {
    await _query(task).drain<void>();
    _notifyChanged(affectedTables);
  }

  /// Watch [tables], returning a [Stream] that emits the result of [fetch]
  /// as soon as possible, and again every time a write to one of [tables]
  /// happens through this [Database].
  ///
  /// Each listener gets its own independent lifecycle: subscribing always
  /// triggers a fresh [fetch] call, and the underlying subscription to table
  /// changes is cancelled once that listener stops listening.
  ///
  /// At-most one call to [fetch] is ever in-flight per listener. If changes
  /// arrive while a call to [fetch] is still running, a single follow-up
  /// call is made once it completes, rather than racing multiple concurrent
  /// calls to [fetch] (whose results could otherwise arrive out of order).
  Stream<R> _watch<R>(Set<String> tables, Future<R> Function() fetch) {
    return Stream.multi((controller) {
      var isFetching = false;
      var isDirty = true; // Trigger one call to `fetch` immediately.

      Future<void> pump() async {
        if (isFetching) {
          return;
        }
        isFetching = true;
        try {
          while (isDirty) {
            isDirty = false;
            try {
              controller.addSync(await fetch());
            } catch (error, stackTrace) {
              controller.addErrorSync(error, stackTrace);
            }
          }
        } finally {
          isFetching = false;
        }
      }

      final subscription = _tableChanges
          .where((changed) => changed.any(tables.contains))
          .listen((_) {
            isDirty = true;
            unawaited(pump());
          });

      controller.onCancel = subscription.cancel;

      unawaited(pump());
    }, isBroadcast: true);
  }

  /// Create a [QuerySingle] that evaluates [expressions].
  ///
  /// Returns a [QuerySingle] with exactly one row.
  ///
  /// The values in [expressions] **must** be [Expr] objects. If you pass any
  /// other record as [expressions], there will be no `.fetch()`
  /// _extension method_ for fetching results.
  ///
  /// This can be useful for evaluating multiple point-queries in a single
  /// database query.
  QuerySingle<S> select<S extends Record>(S expressions) =>
      QuerySingle._(Query._(this, expressions, SelectClause._));
}
