"""Peewee migrations -- 001_auto.py.

Some examples (model - class or model name)::

    > Model = migrator.orm['table_name']            # Return model in current state by name
    > Model = migrator.ModelClass                   # Return model in current state by name

    > migrator.sql(sql)                             # Run custom SQL
    > migrator.run(func, *args, **kwargs)           # Run python function with the given args
    > migrator.create_model(Model)                  # Create a model (could be used as decorator)
    > migrator.remove_model(model, cascade=True)    # Remove a model
    > migrator.add_fields(model, **fields)          # Add fields to a model
    > migrator.change_fields(model, **fields)       # Change fields
    > migrator.remove_fields(model, *field_names, cascade=True)
    > migrator.rename_field(model, old_field_name, new_field_name)
    > migrator.rename_table(model, new_table_name)
    > migrator.add_index(model, *col_names, unique=False)
    > migrator.add_not_null(model, *field_names)
    > migrator.add_default(model, field_name, default)
    > migrator.add_constraint(model, name, sql)
    > migrator.drop_index(model, *col_names)
    > migrator.drop_not_null(model, *field_names)
    > migrator.drop_constraints(model, *constraints)

"""

from contextlib import suppress

import peewee as pw
from peewee_migrate import Migrator


with suppress(ImportError):
    import playhouse.postgres_ext as pw_pext


def migrate(migrator: Migrator, database: pw.Database, *, fake=False):
    """Write your migrations here."""
    
    @migrator.create_model
    class User(pw.Model):
        id = pw.AutoField()
        did = pw.CharField(max_length=255, unique=True)
        handle = pw.CharField(max_length=255, null=True)
        followers_count = pw.IntegerField(null=True)
        follows_count = pw.IntegerField(null=True)
        posts_count = pw.IntegerField(null=True)
        indexed_at = pw.DateTimeField()
        last_update = pw.DateTimeField(null=True)

        class Meta:
            table_name = "user"

    @migrator.create_model
    class Post(pw.Model):
        id = pw.AutoField()
        author = pw.ForeignKeyField(column_name='author_id', field='id', model=migrator.orm['user'], null=True)
        uri = pw.CharField(max_length=255, unique=True)
        cid = pw.CharField(index=True, max_length=255)
        reply_parent = pw.CharField(index=True, max_length=255, null=True)
        reply_root = pw.CharField(index=True, max_length=255, null=True)
        indexed_at = pw.DateTimeField()
        created_at = pw.DateTimeField(index=True, null=True)

        class Meta:
            table_name = "post"

    @migrator.create_model
    class Interaction(pw.Model):
        id = pw.AutoField()
        uri = pw.CharField(max_length=255, unique=True)
        cid = pw.CharField(max_length=255)
        author = pw.ForeignKeyField(column_name='author_id', field='id', model=migrator.orm['user'], on_delete='CASCADE')
        post = pw.ForeignKeyField(column_name='post_id', field='id', model=migrator.orm['post'], on_delete='CASCADE')
        interaction_type = pw.IntegerField(index=True)
        indexed_at = pw.DateTimeField()
        created_at = pw.DateTimeField(index=True, null=True)

        class Meta:
            table_name = "interaction"

    @migrator.create_model
    class Language(pw.Model):
        id = pw.AutoField()
        code = pw.CharField(max_length=255, unique=True)

        class Meta:
            table_name = "language"

    @migrator.create_model
    class PostLanguageThrough(pw.Model):
        id = pw.AutoField()
        post = pw.ForeignKeyField(column_name='post_id', field='id', model=migrator.orm['post'])
        language = pw.ForeignKeyField(column_name='language_id', field='id', model=migrator.orm['language'])

        class Meta:
            table_name = "post_language_through"
            indexes = [(('post', 'language'), True)]


def rollback(migrator: Migrator, database: pw.Database, *, fake=False):
    """Write your rollback migrations here."""
    
    migrator.remove_model('post_language_through')

    migrator.remove_model('language')

    migrator.remove_model('interaction')

    migrator.remove_model('post')

    migrator.remove_model('user')
