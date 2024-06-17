import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from config import Config


class ExtHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        self.ext = ['.csv', '.json', '.txt']

    @staticmethod
    def _get_ext(src_path: str) -> str:
        items = src_path.split('.')
        return f'.{items[-1]}' if len(items) > 1 else ''

    def on_modified(self, event):
        if self._get_ext(event.src_path) in self.ext:
            print(f"File {event.src_path} has been modified")

    def on_moved(self, event):
        if self._get_ext(event.src_path) in self.ext:
            print(f"File {event.src_path} has been moved")

    def on_created(self, event):
        if self._get_ext(event.src_path) in self.ext:
            print(f"File {event.src_path} has been created")

    def on_deleted(self, event):
        if self._get_ext(event.src_path) in self.ext:
            print(f"File {event.src_path} has been deleted")


def watch_file_system():
    """Start watching File System """
    # Specify the path of the directory you want to watch
    directory_path = Config().app_dir + '/data'

    # Create an observer and pass in the directory path and the event handler
    observer = Observer()
    observer.schedule(ExtHandler(), directory_path, recursive=True)

    # Start the observer
    observer.start()

    # Wait until the observer is stopped
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f'file system watcher stopped')
    finally:
        observer.stop()   # Stop
        observer.join()   # Clean up


if __name__ == '__main__':
    watch_file_system()
