#!/usr/bin/env python3
"""
Career Document Processing Script

A simple runner script to process job responsibilities, meeting notes, presentation slides, tender, proposal documents using MarkItDown and store them in PostgreSQL
with intelligent document structure-based chunking.
"""

import sys
import os

# Add the data_preprocessing directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'data_preprocessing'))

from enhanced_preprocessing import main

if __name__ == "__main__":
    # Set default arguments for career document processing
    if len(sys.argv) == 1:
        # If no arguments provided, set up defaults for career processing
        sys.argv.extend([
            "--directory", "'/Volumes/homes/Erathia/Career/13 VisionTech/03 Ops/03 Powerpoint/01 Presales Deck/01 PSG'",
            "--create-schema",
            "--chunk-size", "800",  # Smaller chunks for better semantic coherence
            "--max-chunk-size", "1500",
            "--batch-size", "50"  # Smaller batches for more frequent progress updates
        ])

        print("🚀 Running Career Document Processing with default settings:")
        print("   Directory: '/Volumes/homes/Erathia/Career/13 VisionTech/03 Ops/03 Powerpoint/01 Presales Deck/01 PSG'")
        print("   Chunk size: 800 characters (target)")
        print("   Max chunk size: 1500 characters")
        print("   Creating database schema if needed")
        print("   Processing with MarkItDown + Document Structure-Based Chunking")
        print()

    # Run the main processing function
    sys.exit(main())