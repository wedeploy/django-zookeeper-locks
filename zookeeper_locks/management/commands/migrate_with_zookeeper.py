"""An atomic version of the original migrate management command."""

from contextlib import contextmanager

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.management.commands.migrate import Command as BaseCommand
from django.db import connection
from django.db.migrations.executor import MigrationExecutor

from zookeeper_locks.locks import Lock


@contextmanager
def nullcontext():
    yield


class Command(BaseCommand):

    """A migrate management command that executes in a critical section."""

    def __init__(self, lock=None, *args, **kwarg):
        self.lock = lock or Lock('migrations')
        super(Command, self).__init__(*args, **kwarg)

    def launched_with_defaults(self, **options):
        return (
            all(not options.get(opt) for opt in ['settings', 'pythonpath', 'app_label', 'migration_name', 'fake', 'fake_initial', 'run_syncdb'])
            and options.get('database') == 'default'
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.MIGRATE_HEADING("Potential data migration will be assisted by Zookeeper locks."))
        try:
            executor = MigrationExecutor(connection)
        except ImproperlyConfigured:
            # No databases are configured (or the dummy one)
            has_unapplied_migrations = True  # let the superclass decide what to do
        else:
            has_unapplied_migrations = bool(executor.migration_plan(executor.loader.graph.leaf_nodes()))

        if not self.launched_with_defaults(**options) or has_unapplied_migrations:
            if settings.ZOOKEEPER_HOSTS:
                lock_ctx = self.lock
            else:
                self.stdout.write(
                    self.style.NOTICE(
                        "No host has been defined in `settings.ZOOKEEPER_HOSTS` - Zookeeper locks will not protect current data migration process."
                    )
                )
                lock_ctx = nullcontext
            with lock_ctx():
                super(Command, self).handle(*args, **options)
        else:
            self.stdout.write(self.style.MIGRATE_LABEL("No migrations to apply."))
