# workRAG

A Retrieval-Augmented Generation (RAG) system that provides intelligent knowledge base querying through an MCP (Model Context Protocol) server, designed to enhance development workflows with contextual information retrieval.

## 🚀 Overview

workRAG is a two-component system designed to:
1. **Preprocess and chunk documents** for optimal retrieval
2. **Serve contextual information** to development tools like Cursor via MCP protocol

## 🏗️ Architecture

### Components

#### 1. Enhanced Data Preprocessing Pipeline
- **Microsoft MarkItDown Integration**: Converts PDFs, Word docs, PowerPoint, Excel, and more to structured Markdown
- **Generic Document Structure-Based Chunking**: Intelligent chunking that preserves document structure and meaning, suitable for all types of work documents (presentations, proposals, meeting notes, etc.)
- **Automated scheduling**: Weekly data processing runs
- **Database integration**: RAG-optimized PostgreSQL storage with content and chunks tables

#### 2. MCP Server
- **Protocol**: Model Context Protocol (Anthropic)
- **Purpose**: Real-time RAG information retrieval
- **Integration**: Direct connection to Cursor IDE

### Data Flow

```
User Query → MCP Server → Database Query → Top 10 Chunks → LLM Context → Response
```

1. **User Input**: Receives queries from development tools
2. **Information Retrieval**: Searches PostgreSQL knowledge base
3. **Context Selection**: Retrieves top 10 relevant information chunks
4. **LLM Enhancement**: Provides context to language models
5. **Response Delivery**: Returns enhanced responses to development tools

## 🛠️ Tech Stack

- **Database**: PostgreSQL
- **Infrastructure**: Synology NAS
- **Protocol**: Model Context Protocol (MCP)
- **Integration**: Cursor IDE

## 📋 Prerequisites

- PostgreSQL database
- Synology NAS (optional, for infrastructure)
- Cursor IDE (for MCP integration)
- Python 3.8+

## 🚀 Installation

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/workRAG.git
cd workRAG
```

### 2. Setup Environment
```bash
# Create virtual environment
conda create -n workRag

conda activate workRag

# Install dependencies
pip install -r requirements.txt
```

### 3. Database Setup
```bash
# Configure PostgreSQL connection
# Update database credentials in config file
```

### 4. Configure MCP Server
```bash
# Setup MCP server configuration
# Configure Cursor integration
```

## 🔧 Usage

### Enhanced Data Preprocessing with MarkItDown
```bash
# Quick start for document processing (with defaults)
python run_career_processing.py

# Run with custom settings
python data_preprocessing/enhanced_preprocessing.py --directory "/path/to/documents" --chunk-size 800

# Legacy metadata-only processing
python data_preprocessing/preprocessing.py

# Database setup (run once)
python data_preprocessing/setup_database.py

# Schedule weekly runs
# Configure cron job or task scheduler
```

#### Processing Features:
- **File Type Support**: PDF, DOCX, PPTX, XLSX, TXT, HTML, images (with OCR), audio (with transcription)
- **Chunking Strategies**: Generic structure-based chunking for all document types
- **Database Schema**: Files metadata, document content, and semantic chunks
- **Progress Tracking**: Real-time progress bars and detailed logging

### MCP Server
```bash
# Start MCP server
python MCP_server/server.py

# The server will be available for Cursor integration
```

## 📁 Project Structure

```
workRAG/
├── data_preprocessing/     # Data processing pipeline
├── MCP_server/            # Model Context Protocol server
├── readme.md              # Project documentation
└── .gitignore            # Git ignore rules
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Links

- [Model Context Protocol Documentation](https://spec.modelcontextprotocol.io/)
- [Cursor IDE](https://cursor.sh/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## 📞 Support

For support, please open an issue in the GitHub repository or contact the maintainers.

## Clearing the Database

To remove all data from your PostgreSQL database (including all file metadata, content, and chunks), use the provided script:

```bash
python data_preprocessing/clear_database.py
```

You will be prompted for confirmation before any data is deleted. This operation cannot be undone.

**Warning:** This will permanently delete all data from the following tables:
- content_chunks
- document_content
- file_metadata
- scan_sessions
- file_duplicates
