import os
import argparse
from concurrent.futures import ThreadPoolExecutor
import traceback
from time import time
import zipfile

import py7zr
import rarfile
import zstd

ignore = ["Vimm's Lair.txt"]



def display_file_size(size_bytes: int) -> str:
    
    # Define the thresholds for KB, MB, GB, and TB
    KB = 1024
    MB = KB ** 2  # 1,048,576
    GB = KB ** 3  # 1,073,741,824
    TB = KB ** 4  # 1,099,511,627,776

    # Format the size based on the thresholds
    if size_bytes < KB:
        return f"{size_bytes} bytes"
    elif size_bytes < MB:
        return f"{size_bytes / KB:.2f} KB"
    elif size_bytes < GB:
        return f"{size_bytes / MB:.2f} MB"
    elif size_bytes < TB:
        return f"{size_bytes / GB:.2f} GB"
    else:
        return f"{size_bytes / TB:.2f} TB"

# Extract Functions
# _________________

archive_types = ["7z", "zip", "rar", "gz"]


def smart_extract_rom(
    extract_func,
    files: list[str],
    archive: str,
    ignore: list[str] = [],
):
    archive_name = os.path.basename(archive).split(".")[0]
    archive_dir = os.path.dirname(os.path.abspath(archive))

    # Smart extraction, extract single file into root directory, or multiple files into a directory
    extract_files = []
    for f in files:
        if f.endswith("/"):
            continue
        elif f.split("/")[-1] in ignore:
            continue
        extract_files.append(f)

    # If more than 1 file, extract into directory
    if len(extract_files) > 1:
        add_prefix = None
        extract_path = os.path.join(archive_dir)
        remove_prefix = os.path.commonprefix(extract_files)
        if not remove_prefix.endswith("/"):
            remove_prefix = remove_prefix[0 : remove_prefix.rfind("/") + 1]

        # Allow only last directory in remove_prefix, add folder if no common prefix
        if remove_prefix == "":
            remove_prefix = None
            extract_path = os.path.join(archive_dir, archive_name)
        elif "/" not in remove_prefix[:-1]:
            add_prefix = remove_prefix[:-1]
            remove_prefix = None
        else:
            remove_prefix = "/".join(remove_prefix.split("/")[0:-2]) + "/"

        # Start extraction, then move files if necessary
        extract_func(extract_files, extract_path)
        for f in extract_files:
            if remove_prefix:
                to = os.path.join(extract_path, f[0 : len(remove_prefix)])
                os.makedirs(os.path.dirname(to), exist_ok=True)
                os.rename(os.path.join(extract_path, f), to)
        if remove_prefix:
            os.rmdir(os.path.join(extract_path, remove_prefix.split("/")[0]))

        # Return directory of extracted files
        return os.path.join(extract_path, add_prefix) if add_prefix else extract_path

    # Extract single file into root directory
    else:
        f = extract_files[0]
        f_filename = f.split("/")[-1]
        extract_func(targets=[f], path=archive_dir)
        if "/" in f:
            os.rename(
                os.path.join(archive_dir, f),
                os.path.join(archive_dir, f_filename),
            )
            os.rmdir(os.path.join(archive_dir, f.split("/")[0]))
        # Return single file extracted
        return os.path.join(archive_dir, f_filename)


def extract_zip(archive: str, ignore: list[str] = []):
    with zipfile.ZipFile(archive, "r") as z:

        def extract(targets: list[str], path: str):
            for target in targets:
                z.extract(target, path)

        files = z.namelist()
        output = smart_extract_rom(extract, files, archive, ignore)
    return output


def extract_7z(archive: str, ignore: list[str] = []):
    with py7zr.SevenZipFile(archive, mode="r") as z:

        def extract(targets: list[str], path: str):
            z.extract(targets=targets, path=path)

        files = z.getnames()
        output = smart_extract_rom(extract, files, archive, ignore)
    return output


def extract_rar(archive: str, ignore: list[str] = []):
    with rarfile.RarFile(archive, mode="r") as z:

        def extract(targets: list[str], path: str):
            for target in targets:
                z.extract(target, path)

        files = z.namelist()
        output = smart_extract_rom(extract, files, archive, ignore)
    return output


extract_funcs = {
    "7z": extract_7z,
    "zip": extract_zip,
    "rar": extract_rar,
}

# Compress Functions
# __________________


def compress_zstd(file: str):
    output = file + ".zst"
    with open(file, "rb") as input_f, open(output, "wb") as output_f:
        output_f.write(zstd.ZSTD_compress(input_f.read(), int(os.environ.get("ZSTD_LEVEL", 6)), 0))
    return output

def compress_zarchive(file: str):
    # Check if 'zarchive' command is available
    if os.system("zarchive --version") != 0:
        raise ValueError("zarchive command not found, please install it")
    

compress_types = {
    "zar": compress_zstd,
    "zst": compress_zstd,
    # "chd": compress_chd,
}

compress_types_install_checks = {
    "zar": ("check_command", "zarchive"),
}

compress_types_are_installed = {
    "zar": None,
}

parent_dir_compress = {
    "xbox360": ("iso", "zar"),
}

# Main Functions
# ______________


def process_rom(rom: str, ignore: list[str], clean_up: bool) -> tuple[str, str]:
    input = rom
    is_dir = os.path.isdir(rom)
    ext = rom.split(".")[-1]

    # Extract archive
    # _______________

    if ext in archive_types:
        if ext not in extract_funcs:
            raise ValueError(f"Archive type '{ext}' not supported")

        extract_start = time()
        extract_path = extract_funcs[ext](rom, ignore)
        if clean_up:
            os.remove(rom)
        rom = extract_path
        is_dir = os.path.isdir(rom)
        print(
            f"+ Extracted {'directory' if is_dir else 'file'} ({time()-extract_start:.2f}s): {rom}"
        )

    # Compress files
    # ______________

    parent_dir_name = os.path.basename(os.path.dirname(os.path.abspath(rom)))
    if parent_dir_name in parent_dir_compress:
        from_ext, to_ext = parent_dir_compress[parent_dir_name]
        if is_dir:
            # Get all nested files as a list
            compress_files = []
            for root, _, f in os.walk(rom):
                for file in f:
                    if file.endswith("." + from_ext):
                        compress_files.append(os.path.join(root, file))
        else:
            compress_files = [rom]

        for f in compress_files:
            compress_start = time()
            compress_file = compress_types[to_ext](f, to_ext)
            size_before = os.path.getsize(f)
            if clean_up:
                os.remove(f)
            size_after = os.path.getsize(compress_file)
            size_ratio = size_after / size_before
            print(
                f"+ Compressed file to {size_ratio*100:.2f}%, {display_file_size(size_after)} ({time() - compress_start:.2f}s): {compress_file}"
            )

        if len(compress_files) == 1:
            rom = compress_file

    # Print results and return
    # ________________________

    if input == rom:
        print(f"= No changes made to file: {rom}")
    else:
        print(f"$ Prepared rom {'directory' if is_dir else 'file'}: {rom}")

    return input, rom


def extract_roms(
    files: list[str] = [],
    dirs: list[str] = [],
    max_workers: int = 1,
    ignore: list[str] = ignore,
    clean_up: bool = False,
):
    if files is None:
        files = []
    if dirs is None:
        dirs = []

    # If no directories or files are provided, exit
    if len(dirs) == 0 and len(files) == 0:
        raise ValueError("No directories or files provided")

    # Get list of all files in 'dirs' and add to 'files' list
    for d in dirs:
        for f in os.listdir(d):
            if os.path.isfile(os.path.join(d, f)):
                files.append(os.path.join(d, f))

    print(f"+ Processing roms ({len(files)}):")
    success = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for f in files:
            futures.append((f, executor.submit(process_rom, f, ignore, clean_up)))

        # Wait for all futures to complete
        for f, future in futures:
            try:
                future.result()
                success += 1
            except Exception as e:
                print(f"Failed to process '{f}':\n{traceback.format_exc()}")

    if success == 0:
        raise RuntimeError("[ERROR] No roms were processed successfully")
    else:
        print(f"[SUCCESS] Processed {success} roms")

    # Based on parent directory, determine if rom can be compressed


def run_args():
    parser = argparse.ArgumentParser(
        description="Extract roms from archives and compress if possible"
    )
    parser.add_argument("files", nargs="*", help="File to extract roms from")
    parser.add_argument(
        "--dirs", "-d", nargs="*", help="Directory to extract roms from"
    )
    parser.add_argument(
        "--max-workers", "-w", type=int, default=1, help="Number of workers to use"
    )
    parser.add_argument(
        "--ignore",
        "-i",
        nargs="*",
        help="When extracting roms, ignore these files",
        default=ignore,
    )
    parser.add_argument(
        "--clean-up",
        "-C",
        action="store_true",
        help="[WARNING: Irreversible] Delete all files except final output (e.g. .7z > .iso > .chd | only keeps '.chd').",
    )
    args = parser.parse_args()
    return extract_roms(**vars(args))


if __name__ == "__main__":
    run_args()
