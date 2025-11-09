#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import zipfile
import sys

def zip_gdb_folders(gdb_folder='data/gdbs'):
    """Zip GDB folders in the specified directory"""
    try:
        # Validate input folder
        if not os.path.exists(gdb_folder):
            print("ERROR: GDB folder not found: {}".format(gdb_folder))
            return False

        # Find GDB folders
        gdb_folders = [f for f in os.listdir(gdb_folder)
                      if os.path.isdir(os.path.join(gdb_folder, f)) and f.endswith('.gdb')]

        if not gdb_folders:
            print("No GDB folders found")
            return False

        success_count = 0
        failure_count = 0

        for gdb_folder_name in gdb_folders:
            gdb_path = os.path.join(gdb_folder, gdb_folder_name)
            zip_path = os.path.join(gdb_folder, "{}.zip".format(gdb_folder_name))

            try:
                # Create ZIP file
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    # Add all files and subdirectories from the GDB folder
                    for root, dirs, files in os.walk(gdb_path):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, gdb_path)
                            zipf.write(file_path, arcname)

                success_count += 1
            except Exception as e:
                failure_count += 1

        print("Created {} ZIPs, {} failed".format(success_count, failure_count))
        return success_count > 0

    except Exception as e:
        print("ERROR: {}".format(e))
        return False

if __name__ == "__main__":
    # Get GDB folder from command line argument if provided
    gdb_folder = sys.argv[1] if len(sys.argv) > 1 else 'data/gdbs'
    success = zip_gdb_folders(gdb_folder)
    sys.exit(0 if success else 1)