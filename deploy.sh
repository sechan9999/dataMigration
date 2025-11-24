#!/bin/bash

###############################################################################
# Deployment Script for Delay Queue System
#
# This script deploys the delay queue system to Azure Databricks
###############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DATABRICKS_HOST="${DATABRICKS_HOST:-https://your-workspace.azuredatabricks.net}"
DATABRICKS_TOKEN="${DATABRICKS_TOKEN}"

# Paths
LOCAL_SRC_DIR="./src"
LOCAL_CONFIG_DIR="./config"
LOCAL_NOTEBOOKS_DIR="./notebooks"

DBFS_SRC_PATH="dbfs:/src"
DBFS_CONFIG_PATH="dbfs:/config"
DBFS_NOTEBOOKS_PATH="dbfs:/notebooks"

###############################################################################
# Functions
###############################################################################

print_header() {
    echo ""
    echo "=========================================================================="
    echo "$1"
    echo "=========================================================================="
    echo ""
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

check_prerequisites() {
    print_header "Checking Prerequisites"

    # Check if databricks CLI is installed
    if ! command -v databricks &> /dev/null; then
        print_error "Databricks CLI not found. Please install it:"
        echo "pip install databricks-cli"
        exit 1
    fi
    print_success "Databricks CLI installed"

    # Check if token is set
    if [ -z "$DATABRICKS_TOKEN" ]; then
        print_error "DATABRICKS_TOKEN environment variable not set"
        echo "Export your token: export DATABRICKS_TOKEN=your-token"
        exit 1
    fi
    print_success "Databricks token configured"

    # Configure databricks CLI
    databricks configure --token <<EOF
$DATABRICKS_HOST
$DATABRICKS_TOKEN
EOF

    print_success "Databricks CLI configured"
}

deploy_source_code() {
    print_header "Deploying Source Code"

    # Remove old files
    print_warning "Removing old source files from DBFS..."
    databricks fs rm -r $DBFS_SRC_PATH 2>/dev/null || true

    # Upload new files
    print_warning "Uploading source code to DBFS..."
    databricks fs cp -r $LOCAL_SRC_DIR $DBFS_SRC_PATH

    print_success "Source code deployed to $DBFS_SRC_PATH"
}

deploy_config() {
    print_header "Deploying Configuration"

    # Remove old config
    print_warning "Removing old config files from DBFS..."
    databricks fs rm -r $DBFS_CONFIG_PATH 2>/dev/null || true

    # Upload new config
    print_warning "Uploading configuration to DBFS..."
    databricks fs cp -r $LOCAL_CONFIG_DIR $DBFS_CONFIG_PATH

    print_success "Configuration deployed to $DBFS_CONFIG_PATH"
}

deploy_notebooks() {
    print_header "Deploying Notebooks"

    # Remove old notebooks
    print_warning "Removing old notebooks from DBFS..."
    databricks fs rm -r $DBFS_NOTEBOOKS_PATH 2>/dev/null || true

    # Upload new notebooks
    print_warning "Uploading notebooks to DBFS..."
    databricks fs cp -r $LOCAL_NOTEBOOKS_DIR $DBFS_NOTEBOOKS_PATH

    print_success "Notebooks deployed to $DBFS_NOTEBOOKS_PATH"
}

verify_deployment() {
    print_header "Verifying Deployment"

    # Check source files
    print_warning "Checking source files..."
    if databricks fs ls $DBFS_SRC_PATH/models > /dev/null 2>&1; then
        print_success "Source files verified"
    else
        print_error "Source files not found"
        exit 1
    fi

    # Check config files
    print_warning "Checking config files..."
    if databricks fs ls $DBFS_CONFIG_PATH/queue_config.yaml > /dev/null 2>&1; then
        print_success "Config files verified"
    else
        print_error "Config files not found"
        exit 1
    fi

    # Check notebooks
    print_warning "Checking notebooks..."
    if databricks fs ls $DBFS_NOTEBOOKS_PATH > /dev/null 2>&1; then
        print_success "Notebooks verified"
    else
        print_error "Notebooks not found"
        exit 1
    fi
}

display_summary() {
    print_header "Deployment Summary"

    echo "Deployment completed successfully!"
    echo ""
    echo "Next steps:"
    echo ""
    echo "1. Open Databricks workspace: $DATABRICKS_HOST"
    echo ""
    echo "2. Import notebooks from DBFS:"
    echo "   - Navigate to Workspace"
    echo "   - Import from $DBFS_NOTEBOOKS_PATH"
    echo ""
    echo "3. Run initialization notebook:"
    echo "   - 01_setup_and_initialization.py"
    echo ""
    echo "4. Configure your storage paths in:"
    echo "   - $DBFS_CONFIG_PATH/queue_config.yaml"
    echo ""
    echo "5. Start using the system:"
    echo "   - Enqueue tasks: 02_enqueue_migration_tasks.py"
    echo "   - Process queue: 03_process_queue.py"
    echo "   - Monitor: 04_monitoring_dashboard.py"
    echo ""
    print_success "Happy migrating! 🚀"
}

###############################################################################
# Main
###############################################################################

main() {
    print_header "Delay Queue System - Deployment Script"

    check_prerequisites
    deploy_source_code
    deploy_config
    deploy_notebooks
    verify_deployment
    display_summary
}

# Run main function
main
