# FastCopy - Fast MySQL Database Copying Tool

A high-performance bash script for efficiently copying MySQL databases from remote servers to local Docker containers using SSH tunneling and MySQL Shell's parallel dump/load utilities.

## Why It Was Created

FastCopy was created to solve the common problem of copying production databases to local development environments quickly and efficiently. Traditional methods like `mysqldump` can be slow for large databases, and setting up secure connections to remote databases can be complex. This tool addresses these issues by:

- **Performance**: Leveraging MySQL Shell's parallel dump and load capabilities for faster transfers
- **Security**: Using SSH tunneling to securely access remote databases
- **Simplicity**: Providing a single script with configuration file for easy setup
- **Flexibility**: Supporting schema renaming, compression options, and various performance tuning parameters
- **Safety**: Implementing proper error handling, cleanup, and rollback mechanisms

## How It Can Be Used

### Basic Usage

```bash
./fastcopy.sh /path/to/your-config.cfg
```

### Common Use Cases

1. **Development Environment Setup**: Copy production data to local Docker MySQL instances for development and testing
2. **Database Migration**: Transfer databases between different environments with optional schema renaming
3. **Backup and Restore**: Create compressed backups and restore them to different targets
4. **Data Synchronization**: Keep development databases in sync with production data

### Configuration

Create a configuration file (see `sample.cfg` for reference) with the following sections:

#### SSH Configuration
```bash
REMOTE_HOST=your-server.com
REMOTE_SSH_USER=ubuntu
SSH_PORT=22
SSH_IDENTITY_FILE=~/.ssh/id_rsa  # optional
SSH_STRICT_HOST_KEY_CHECKING=no
```

#### Source Database (Remote)
```bash
REMOTE_DB_HOST=127.0.0.1
REMOTE_DB_PORT=3306
REMOTE_DB_USER=dbuser
REMOTE_DB_PASSWORD=password
SOURCE_DB_NAME=production_db
```

#### Target Database (Local Docker)
```bash
TARGET_DOCKER_CONTAINER=mysql8-container
TARGET_DB_USER=root
TARGET_DB_PASSWORD=password
TARGET_DB_NAME=development_db
```

#### Performance Tuning (Optional)
```bash
DUMP_THREADS=8                    # Parallel dump threads
LOAD_THREADS=8                    # Parallel load threads
DUMP_COMPRESSION=zstd             # zstd|gzip|none
DEFER_INDEXES=all                 # none|all|fulltext|secondary
IGNORE_EXISTING=true              # Skip existing objects
KEEP_DUMP=false                   # Keep dump files after load
```

### Example Workflow

1. Copy your production database to a local development container:
```bash
# Start your MySQL Docker container
docker run -d --name mysql-dev -p 3307:3306 -e MYSQL_ROOT_PASSWORD=dev-password mysql:8.0

# Configure fastcopy.cfg with your settings
cp sample.cfg my-project.cfg
# Edit my-project.cfg with your database details

# Run the copy
./fastcopy.sh my-project.cfg
```

2. The script will:
   - Establish an SSH tunnel to your remote server
   - Create a parallel dump of the source database
   - Transfer and load the data into your local Docker container
   - Clean up temporary files and close connections

## Prerequisites

### Required Software

Before running FastCopy, ensure you have the following installed on your system:

#### 1. **SSH Client**
- Standard on macOS and Linux
- Used for creating secure tunnels to remote databases

#### 2. **Docker**
- **Installation**: [Get Docker](https://docs.docker.com/get-docker/)
- **Purpose**: To run the target MySQL container
- **Note**: Your target container must expose port 3306 (e.g., `docker run -p 3307:3306 mysql:8.0`)

#### 3. **MySQL Shell (mysqlsh)**
- **Required version**: 8.0 or later
- **Installation**:
  
  **macOS (Homebrew):**
  ```bash
  brew install mysql-shell
  ```
  
  **Ubuntu/Debian:**
  ```bash
  wget https://dev.mysql.com/get/mysql-apt-config_0.8.34-1_all.deb
  sudo dpkg -i mysql-apt-config_0.8.34-1_all.deb
  sudo apt-get update
  sudo apt-get install mysql-shell
  ```
  
  **CentOS/RHEL:**
  ```bash
  sudo yum install mysql-shell
  ```
  
  **From Source/Binary**: [MySQL Shell Downloads](https://dev.mysql.com/downloads/shell/)

#### 4. **Standard Unix Tools**
These are typically pre-installed on macOS and Linux:
- `bash` (version 4.0+)
- `ssh`
- `docker`
- `ps`, `grep`, `kill` (process management)
- `lsof` or `ss` (port checking)

### System Requirements

- **Operating System**: macOS, Linux, or WSL2 on Windows
- **Memory**: Sufficient RAM for parallel operations (recommended: 4GB+)
- **Disk Space**: Temporary space for database dumps (size depends on your database)
- **Network**: SSH access to remote database server

### Permissions Required

#### On Local Machine:
- Docker access (user should be in `docker` group or have sudo access)
- SSH key access to remote server
- Write permissions to temporary directory (default: `/tmp`)

#### On Remote Server:
- SSH access with key-based authentication
- MySQL user with `SELECT` and `LOCK TABLES` privileges on source database

#### On Target Docker Container:
- MySQL user with `CREATE`, `DROP`, `INSERT`, `ALTER` privileges
- `local_infile` capability (script handles this automatically)

### Network Requirements

- SSH access (default port 22) to remote server
- MySQL port (default 3306) accessible on remote server
- Docker container with exposed MySQL port (e.g., 3307:3306)

### Verification

Test your setup with these commands:

```bash
# Verify MySQL Shell installation
mysqlsh --version

# Verify Docker access
docker ps

# Verify SSH access to remote server
ssh your-user@your-server.com "echo 'SSH connection successful'"

# Verify MySQL Shell can connect to both source and target
mysqlsh --uri user:pass@remote-host:3306 -e "SELECT 1"
mysqlsh --uri root:pass@localhost:3307 -e "SELECT 1"
```

## Features

- ✅ **Parallel Processing**: Multi-threaded dump and load operations
- ✅ **Secure Connections**: SSH tunneling for remote database access
- ✅ **Compression Support**: zstd, gzip, or no compression options
- ✅ **Schema Renaming**: Copy with different database names
- ✅ **Performance Tuning**: Configurable thread counts and index deferring
- ✅ **Error Handling**: Comprehensive error checking and cleanup
- ✅ **Progress Monitoring**: Real-time progress reporting during operations
- ✅ **Automatic Cleanup**: Removes temporary files and closes connections
- ✅ **Cross-Platform**: Works on macOS, Linux, and WSL2

## License

This project is open source. Please check the repository for license details.

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

## Support

For issues or questions, please check the project's issue tracker or create a new issue with:
- Your configuration (with sensitive data removed)
- Error messages or logs
- System information (OS, MySQL Shell version, etc.)