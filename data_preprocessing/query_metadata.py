#!/usr/bin/env python3
"""
File Metadata Query Utility

This script provides various query capabilities for the stored file metadata.
"""

import argparse
import sys
import logging
from typing import List, Dict, Any
from database import DatabaseManager
from config import Config

def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

class MetadataQuery:
    """Class for querying file metadata"""

    def __init__(self):
        self.db_manager = DatabaseManager()
        if not self.db_manager.connect():
            raise Exception("Failed to connect to database")

    def __del__(self):
        if hasattr(self, 'db_manager'):
            self.db_manager.disconnect()

    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics from the database"""
        try:
            stats = {}

            # Total counts
            self.db_manager.cursor.execute("SELECT COUNT(*) FROM file_metadata WHERE is_directory = FALSE")
            stats['total_files'] = self.db_manager.cursor.fetchone()[0]

            self.db_manager.cursor.execute("SELECT COUNT(*) FROM file_metadata WHERE is_directory = TRUE")
            stats['total_directories'] = self.db_manager.cursor.fetchone()[0]

            # Total size
            self.db_manager.cursor.execute("SELECT SUM(file_size) FROM file_metadata WHERE is_directory = FALSE")
            result = self.db_manager.cursor.fetchone()[0]
            stats['total_size'] = result if result else 0

            # File extension distribution
            self.db_manager.cursor.execute("""
                SELECT file_extension, COUNT(*) as count
                FROM file_metadata
                WHERE is_directory = FALSE AND file_extension IS NOT NULL
                GROUP BY file_extension
                ORDER BY count DESC
                LIMIT 20
            """)
            stats['top_extensions'] = self.db_manager.cursor.fetchall()

            # Directory depth distribution
            self.db_manager.cursor.execute("""
                SELECT depth_level, COUNT(*) as count
                FROM file_metadata
                GROUP BY depth_level
                ORDER BY depth_level
            """)
            stats['depth_distribution'] = self.db_manager.cursor.fetchall()

            # Largest files
            self.db_manager.cursor.execute("""
                SELECT file_path, file_size, file_name
                FROM file_metadata
                WHERE is_directory = FALSE AND file_size IS NOT NULL
                ORDER BY file_size DESC
                LIMIT 10
            """)
            stats['largest_files'] = self.db_manager.cursor.fetchall()

            return stats

        except Exception as e:
            logging.error(f"Failed to get summary stats: {e}")
            return {}

    def search_files(self, pattern: str, extension: str = None, min_size: int = None, max_size: int = None) -> List[Dict]:
        """Search for files based on various criteria"""
        try:
            query = "SELECT * FROM file_metadata WHERE is_directory = FALSE"
            params = []

            if pattern:
                query += " AND (file_name ILIKE %s OR file_path ILIKE %s)"
                params.extend([f"%{pattern}%", f"%{pattern}%"])

            if extension:
                query += " AND file_extension = %s"
                params.append(extension)

            if min_size:
                query += " AND file_size >= %s"
                params.append(min_size)

            if max_size:
                query += " AND file_size <= %s"
                params.append(max_size)

            query += " ORDER BY file_path LIMIT 100"

            self.db_manager.cursor.execute(query, params)
            columns = [desc[0] for desc in self.db_manager.cursor.description]
            results = []

            for row in self.db_manager.cursor.fetchall():
                results.append(dict(zip(columns, row)))

            return results

        except Exception as e:
            logging.error(f"Failed to search files: {e}")
            return []

    def find_duplicates(self) -> List[Dict]:
        """Find duplicate files based on file hash"""
        try:
            query = """
                SELECT file_hash, array_agg(file_path) as file_paths, COUNT(*) as duplicate_count, file_size
                FROM file_metadata
                WHERE is_directory = FALSE AND file_hash IS NOT NULL
                GROUP BY file_hash, file_size
                HAVING COUNT(*) > 1
                ORDER BY duplicate_count DESC, file_size DESC
                LIMIT 50
            """

            self.db_manager.cursor.execute(query)
            columns = [desc[0] for desc in self.db_manager.cursor.description]
            results = []

            for row in self.db_manager.cursor.fetchall():
                results.append(dict(zip(columns, row)))

            return results

        except Exception as e:
            logging.error(f"Failed to find duplicates: {e}")
            return []

    def get_directory_contents(self, directory_path: str) -> List[Dict]:
        """Get contents of a specific directory"""
        try:
            query = """
                SELECT * FROM file_metadata
                WHERE parent_directory = %s
                ORDER BY is_directory DESC, file_name
            """

            self.db_manager.cursor.execute(query, (directory_path,))
            columns = [desc[0] for desc in self.db_manager.cursor.description]
            results = []

            for row in self.db_manager.cursor.fetchall():
                results.append(dict(zip(columns, row)))

            return results

        except Exception as e:
            logging.error(f"Failed to get directory contents: {e}")
            return []

def print_summary_stats(stats: Dict[str, Any]):
    """Print summary statistics"""
    print("\n" + "="*60)
    print("FILE METADATA SUMMARY STATISTICS")
    print("="*60)

    print(f"Total Files: {stats.get('total_files', 0):,}")
    print(f"Total Directories: {stats.get('total_directories', 0):,}")

    total_size = stats.get('total_size', 0)
    size_gb = total_size / (1024**3) if total_size else 0
    print(f"Total Size: {total_size:,} bytes ({size_gb:.2f} GB)")

    print("\nTop File Extensions:")
    for ext, count in stats.get('top_extensions', [])[:10]:
        print(f"  {ext}: {count:,} files")

    print("\nDirectory Depth Distribution:")
    for depth, count in stats.get('depth_distribution', []):
        print(f"  Depth {depth}: {count:,} items")

    print("\nLargest Files:")
    for file_path, file_size, file_name in stats.get('largest_files', []):
        size_mb = file_size / (1024**2) if file_size else 0
        print(f"  {file_name}: {size_mb:.2f} MB")

def print_search_results(results: List[Dict]):
    """Print search results"""
    print(f"\nFound {len(results)} matching files:")
    print("-" * 80)

    for item in results:
        size_mb = item['file_size'] / (1024**2) if item['file_size'] else 0
        print(f"File: {item['file_name']}")
        print(f"Path: {item['file_path']}")
        print(f"Size: {size_mb:.2f} MB")
        print(f"Modified: {item['modified_at']}")
        print("-" * 40)

def print_duplicates(duplicates: List[Dict]):
    """Print duplicate files"""
    print(f"\nFound {len(duplicates)} sets of duplicate files:")
    print("-" * 80)

    total_wasted_space = 0

    for dup in duplicates:
        file_size = dup['file_size'] or 0
        count = dup['duplicate_count']
        wasted_space = file_size * (count - 1)
        total_wasted_space += wasted_space

        print(f"Hash: {dup['file_hash'][:16]}...")
        print(f"Size: {file_size / (1024**2):.2f} MB each")
        print(f"Duplicates: {count}")
        print(f"Wasted space: {wasted_space / (1024**2):.2f} MB")
        print("Files:")
        for path in dup['file_paths']:
            print(f"  - {path}")
        print("-" * 40)

    print(f"\nTotal wasted space: {total_wasted_space / (1024**3):.2f} GB")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Query file metadata from PostgreSQL')
    parser.add_argument('--stats', action='store_true', help='Show summary statistics')
    parser.add_argument('--search', help='Search for files by name pattern')
    parser.add_argument('--extension', help='Filter by file extension')
    parser.add_argument('--min-size', type=int, help='Minimum file size in bytes')
    parser.add_argument('--max-size', type=int, help='Maximum file size in bytes')
    parser.add_argument('--duplicates', action='store_true', help='Find duplicate files')
    parser.add_argument('--directory', help='Show contents of specific directory')

    args = parser.parse_args()

    if not any([args.stats, args.search, args.duplicates, args.directory]):
        parser.print_help()
        return 1

    setup_logging()

    try:
        query_tool = MetadataQuery()

        if args.stats:
            stats = query_tool.get_summary_stats()
            print_summary_stats(stats)

        if args.search or args.extension or args.min_size or args.max_size:
            results = query_tool.search_files(
                pattern=args.search,
                extension=args.extension,
                min_size=args.min_size,
                max_size=args.max_size
            )
            print_search_results(results)

        if args.duplicates:
            duplicates = query_tool.find_duplicates()
            print_duplicates(duplicates)

        if args.directory:
            contents = query_tool.get_directory_contents(args.directory)
            print(f"\nContents of {args.directory}:")
            print("-" * 80)
            for item in contents:
                type_str = "DIR" if item['is_directory'] else "FILE"
                size_str = ""
                if not item['is_directory'] and item['file_size']:
                    size_str = f" ({item['file_size'] / (1024**2):.2f} MB)"
                print(f"{type_str}: {item['file_name']}{size_str}")

        return 0

    except Exception as e:
        logging.error(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())