
import os

class FileUtils:
    def delete_directory(self, directory_path):
        try:
            # Iterate over all files and subdirectories in the directory
            for item in os.listdir(directory_path):
                item_path = os.path.join(directory_path, item)

                # If it's a file, remove it
                if os.path.isfile(item_path):
                    os.remove(item_path)

                # If it's a subdirectory, recursively delete it
                elif os.path.isdir(item_path):
                    self.delete_directory(item_path)

            # Finally, remove the empty directory itself
            os.rmdir(directory_path)
            print(f"Directory '{directory_path}' and its contents have been successfully deleted.")
        except Exception as e:
            print(f"Error occurred while deleting the directory '{directory_path}': {e}")
