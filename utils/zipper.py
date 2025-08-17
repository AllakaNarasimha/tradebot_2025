import zipfile
import os
from datetime import datetime
from tqdm import tqdm

class Zipper:
    def __init__(self, folder_path, target_folder, zip_name):
        self.folder_path = folder_path
        self.zip_name = zip_name
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)

    def zip_folder(self):
        file_count = sum(len(files) for _, _, files in os.walk(self.folder_path))
        
        with tqdm(total=file_count, desc='Zipping', unit='file') as pbar:
            with zipfile.ZipFile(self.zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(self.folder_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, self.folder_path)
                        zipf.write(file_path, arcname)
                        pbar.update(1)

if __name__ == '__main__':
    # Example usage
    folder_to_zip = "data"  # Replace with the actual path to your folder
    sym = "BANKNIFTY"
    target_folder = "backup"
    zip_file_name = os.path.join(target_folder, datetime.now().strftime("BACKUP_{}_%d%m%Y_%H%M%S.zip".format(sym)))
    folder_zipper = Zipper(folder_to_zip, target_folder, zip_file_name)
    folder_zipper.zip_folder()
