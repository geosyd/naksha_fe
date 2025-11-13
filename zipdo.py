#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import zipfile
import sys
import argparse

def zip_gdb_folders(gdb_folder='data/gdbs', use_backup=False):
    """Zip GDB folders in the specified directory"""
    try:
        # Validate input folder
        if not os.path.exists(gdb_folder):
            print("ERROR: GDB folder not found: {}".format(gdb_folder))
            return False

        # Determine output directory
        if use_backup:
            output_dir = os.path.join(gdb_folder, 'backup')
            # Create backup directory if it doesn't exist
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                print("Created backup directory: {}".format(output_dir))
        else:
            output_dir = gdb_folder

        # Find GDB folders
        gdb_folders = [f for f in os.listdir(gdb_folder)
                      if os.path.isdir(os.path.join(gdb_folder, f)) and f.endswith('.gdb')]

        if not gdb_folders:
            print("No GDB folders found")
            return False

        success_count = 0
        failure_count = 0

        print("Zipping GDB folders to: {}".format(output_dir))
        for gdb_folder_name in gdb_folders:
            gdb_path = os.path.join(gdb_folder, gdb_folder_name)
            zip_path = os.path.join(output_dir, "{}.zip".format(gdb_folder_name))

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

def main():
    """Main function to handle command line arguments"""
    parser = argparse.ArgumentParser(description='Zip GDB folders in specified directory')

    # Arguments
    parser.add_argument('gdb_folder', nargs='?', default='data/gdbs',
                       help='GDB folder path (default: data/gdbs)')
    parser.add_argument('--uploaded-gdbs', action='store_true',
                       help='Zip to data/gdbs/backup folder instead of data/gdbs')

    args = parser.parse_args()

    # Call zip function with appropriate arguments
    success = zip_gdb_folders(args.gdb_folder, args.uploaded_gdbs)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()