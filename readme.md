# workRAG

A Retrieval-Augmented Generation (RAG) system that provides intelligent knowledge base querying through an MCP (Model Context Protocol) server, designed to enhance development workflows with contextual information retrieval.

## 🚀 Overview

workRAG is a two-component system designed to:
1. **Preprocess and chunk documents** for optimal retrieval
2. **Serve contextual information** to development tools like Cursor via MCP protocol

## 🏗️ Architecture

### Components

#### 1. Data Preprocessing Pipeline
- **Automated scheduling**: Weekly data processing runs
- **Intelligent chunking**: Optimized information segmentation strategies
- **Database integration**: Seamless PostgreSQL storage

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
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

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

### Data Preprocessing
```bash
# Run data preprocessing pipeline
python data_preprocessing/main.py

# Schedule weekly runs
# Configure cron job or task scheduler
```

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
