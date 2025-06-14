import os
import hashlib
import stat
import pwd
import grp
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Generator, Optional
import magic
import logging
from config import Config

logger = logging.getLogger(__name__)

class FileScanner:
    """Handles file system scanning and metadata extraction"""

    def __init__(self):
        self.stats = {
            'total_files': 0,
            'total_directories': 0,
            'total_size': 0,
            'errors_count': 0,
            'processed_count': 0
        }

        # Initialize file type detector
        try:
            self.magic_mime = magic.Magic(mime=True)
        except Exception as e:
            logger.warning(f"Failed to initialize magic library: {e}")
            self.magic_mime = None

    def calculate_file_hash(self, file_path: str) -> Optional[str]:
        """Calculate SHA256 hash of a file"""
        try:
            file_size = os.path.getsize(file_path)
            if file_size > Config.MAX_FILE_SIZE_FOR_HASH:
                logger.debug(f"Skipping hash for large file: {file_path} ({file_size} bytes)")
                return None

            hash_sha256 = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            logger.error(f"Failed to calculate hash for {file_path}: {e}")
            self.stats['errors_count'] += 1
            return None

    def get_mime_type(self, file_path: str) -> Optional[str]:
        """Get MIME type of a file"""
        try:
            if self.magic_mime:
                return self.magic_mime.from_file(file_path)
            else:
                # Fallback to file extension based detection
                extension = Path(file_path).suffix.lower()
                mime_map = {
                    '.txt': 'text/plain',
                    '.pdf': 'application/pdf',
                    '.jpg': 'image/jpeg',
                    '.png': 'image/png',
                    '.doc': 'application/msword',
                    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                    '.zip': 'application/zip'
                }
                return mime_map.get(extension, 'application/octet-stream')
        except Exception as e:
            logger.error(f"Failed to get MIME type for {file_path}: {e}")
            return None

    def get_file_ownership(self, file_path: str) -> tuple:
        """Get file owner user and group"""
        try:
            stat_info = os.stat(file_path)
            try:
                owner_user = pwd.getpwuid(stat_info.st_uid).pw_name
            except KeyError:
                owner_user = str(stat_info.st_uid)

            try:
                owner_group = grp.getgrgid(stat_info.st_gid).gr_name
            except KeyError:
                owner_group = str(stat_info.st_gid)

            return owner_user, owner_group
        except Exception as e:
            logger.error(f"Failed to get ownership for {file_path}: {e}")
            return "unknown", "unknown"

    def get_file_permissions(self, file_path: str) -> str:
        """Get file permissions in octal format"""
        try:
            return oct(os.stat(file_path).st_mode)[-3:]
        except Exception as e:
            logger.error(f"Failed to get permissions for {file_path}: {e}")
            return "000"

    def extract_metadata(self, file_path: str, base_path: str, depth: int = 0) -> Dict[str, Any]:
        """Extract comprehensive metadata for a file or directory"""
        try:
            path_obj = Path(file_path)
            stat_info = os.stat(file_path)

            # Basic path information
            relative_path = os.path.relpath(file_path, base_path)
            parent_directory = str(path_obj.parent)

            # Check if it's a directory
            is_directory = os.path.isdir(file_path)

            # File ownership and permissions
            owner_user, owner_group = self.get_file_ownership(file_path)
            permissions = self.get_file_permissions(file_path)

            # Timestamps
            created_at = datetime.fromtimestamp(stat_info.st_ctime)
            modified_at = datetime.fromtimestamp(stat_info.st_mtime)
            accessed_at = datetime.fromtimestamp(stat_info.st_atime)

            metadata = {
                'file_path': file_path,
                'file_name': path_obj.name,
                'file_extension': path_obj.suffix.lower() if not is_directory else None,
                'parent_directory': parent_directory,
                'relative_path': relative_path,
                'is_directory': is_directory,
                'file_size': stat_info.st_size if not is_directory else None,
                'file_hash': None,
                'mime_type': None,
                'created_at': created_at,
                'modified_at': modified_at,
                'accessed_at': accessed_at,
                'depth_level': depth,
                'permissions': permissions,
                'owner_user': owner_user,
                'owner_group': owner_group
            }

            # Additional processing for files (not directories)
            if not is_directory:
                # Calculate file hash
                metadata['file_hash'] = self.calculate_file_hash(file_path)

                # Get MIME type
                metadata['mime_type'] = self.get_mime_type(file_path)

                self.stats['total_files'] += 1
                self.stats['total_size'] += stat_info.st_size
            else:
                self.stats['total_directories'] += 1

            self.stats['processed_count'] += 1

            return metadata

        except Exception as e:
            logger.error(f"Failed to extract metadata for {file_path}: {e}")
            self.stats['errors_count'] += 1
            return None

    def should_skip_item(self, path: str) -> bool:
        """Determine if a file or directory should be skipped"""
        path_obj = Path(path)

        # Skip based on file extension
        if path_obj.suffix.lower() in Config.EXCLUDED_EXTENSIONS:
            return True

        # Skip based on directory name
        if path_obj.name in Config.EXCLUDED_DIRECTORIES:
            return True

        # Skip hidden files/directories (starting with .)
        if path_obj.name.startswith('.') and path_obj.name not in {'.', '..'}:
            return True

        return False

    def scan_directory(self, directory_path: str) -> Generator[Dict[str, Any], None, None]:
        """Recursively scan directory and yield metadata for each item"""
        try:
            if not os.path.exists(directory_path):
                logger.error(f"Directory does not exist: {directory_path}")
                return

            if not os.path.isdir(directory_path):
                logger.error(f"Path is not a directory: {directory_path}")
                return

            logger.info(f"Starting scan of directory: {directory_path}")

            # Process the root directory itself
            root_metadata = self.extract_metadata(directory_path, directory_path, 0)
            if root_metadata:
                yield root_metadata

            # Walk through all subdirectories and files
            for root, dirs, files in os.walk(directory_path):
                # Calculate current depth
                depth = len(Path(root).relative_to(Path(directory_path)).parts)

                # Filter out excluded directories
                dirs[:] = [d for d in dirs if not self.should_skip_item(os.path.join(root, d))]

                # Process directories
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    if not self.should_skip_item(dir_path):
                        metadata = self.extract_metadata(dir_path, directory_path, depth)
                        if metadata:
                            yield metadata

                # Process files
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    if not self.should_skip_item(file_path):
                        metadata = self.extract_metadata(file_path, directory_path, depth)
                        if metadata:
                            yield metadata

                        # Log progress periodically
                        if self.stats['processed_count'] % 100 == 0:
                            logger.info(f"Processed {self.stats['processed_count']} items. "
                                      f"Files: {self.stats['total_files']}, "
                                      f"Directories: {self.stats['total_directories']}, "
                                      f"Errors: {self.stats['errors_count']}")

        except Exception as e:
            logger.error(f"Error scanning directory {directory_path}: {e}")
            self.stats['errors_count'] += 1

    def get_scan_stats(self) -> Dict[str, Any]:
        """Return current scan statistics"""
        return self.stats.copy()

    def reset_stats(self):
        """Reset scan statistics"""
        self.stats = {
            'total_files': 0,
            'total_directories': 0,
            'total_size': 0,
            'errors_count': 0,
            'processed_count': 0
        }