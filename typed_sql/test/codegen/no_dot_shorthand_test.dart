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

import 'test_code_generation.dart';

void main() {
  // Generated code becomes part of the user's library and takes its language
  // version, so it must not use dot shorthands (Dart 3.10+). A package with
  // `sdk: ^3.6.0` otherwise fails to format the generated part.
  testCodeGeneration(
    name: 'Generated upsert and foreign keys name their enums',
    source: r'''
      abstract final class TestDatabase extends Schema {
        Table<Author> get authors;
        Table<Book> get books;
      }

      @PrimaryKey(['authorId'])
      abstract final class Author extends Row {
        int get authorId;
      }

      @PrimaryKey(['bookId'])
      abstract final class Book extends Row {
        int get bookId;

        @References(table: 'authors', field: 'authorId', name: 'author', as: 'books')
        int get authorId;
      }
    ''',
    output: (s) => s
      ..contains('.onConflict(BookConflict.primaryKey)')
      ..contains('.onConflict(AuthorConflict.primaryKey)')
      ..contains('onDelete: ReferentialAction.noAction')
      ..contains('onUpdate: ReferentialAction.noAction')
      ..has(_withoutComments, 'code without comments').which(
        (code) => code
          ..not((c) => c.contains('.onConflict(.'))
          ..not((c) => c.contains(': .noAction')),
      ),
  );
}

/// Doc comments in generated code may show dot-shorthand examples; only the
/// code itself has to avoid them.
String _withoutComments(String source) => source
    .split('\n')
    .where((line) => !line.trimLeft().startsWith('//'))
    .join('\n');
