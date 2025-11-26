#!/usr/bin/env python3
"""
LIMS Migration - Infrastructure Deployment Script
Deploys Azure resources for LIMS migration architecture
"""

import json
import os
import sys
from typing import Dict, Any
import subprocess


class InfrastructureDeployer:
    """Handles deployment of Azure infrastructure"""

    def __init__(self, config_path: str):
        """Initialize deployer with configuration"""
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        self.subscription_id = self.config['azure']['subscription_id']
        self.resource_group = self.config['azure']['resource_group']
        self.location = self.config['azure']['location']

    def run_command(self, command: str) -> tuple:
        """Execute shell command and return output"""
        print(f"Executing: {command}")
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"❌ Error: {result.stderr}")
            return False, result.stderr

        return True, result.stdout

    def login_azure(self):
        """Login to Azure CLI"""
        print("\n" + "="*80)
        print("Azure Login")
        print("="*80)

        success, output = self.run_command("az login")
        if not success:
            raise Exception("Failed to login to Azure")

        # Set subscription
        success, output = self.run_command(
            f"az account set --subscription {self.subscription_id}"
        )
        if not success:
            raise Exception(f"Failed to set subscription {self.subscription_id}")

        print("✓ Azure login successful")

    def create_resource_group(self):
        """Create Azure resource group"""
        print("\n" + "="*80)
        print("Creating Resource Group")
        print("="*80)

        command = f"""
        az group create \
            --name {self.resource_group} \
            --location {self.location} \
            --tags {' '.join([f'{k}={v}' for k, v in self.config['azure']['tags'].items()])}
        """

        success, output = self.run_command(command)
        if success:
            print(f"✓ Resource group '{self.resource_group}' created")
        else:
            print(f"⚠ Resource group may already exist")

    def deploy_data_factory(self):
        """Deploy Azure Data Factory"""
        print("\n" + "="*80)
        print("Deploying Azure Data Factory")
        print("="*80)

        adf_name = self.config['data_factory']['name']

        command = f"""
        az datafactory create \
            --resource-group {self.resource_group} \
            --factory-name {adf_name} \
            --location {self.location}
        """

        success, output = self.run_command(command)
        if success:
            print(f"✓ Data Factory '{adf_name}' deployed")

            # Enable managed identity
            command = f"""
            az datafactory update \
                --resource-group {self.resource_group} \
                --factory-name {adf_name} \
                --set identity.type=SystemAssigned
            """
            self.run_command(command)
            print("✓ Managed Identity enabled")
        else:
            raise Exception("Failed to deploy Data Factory")

    def deploy_storage_account(self):
        """Deploy Azure Data Lake Storage Gen2"""
        print("\n" + "="*80)
        print("Deploying Data Lake Storage")
        print("="*80)

        storage_account = self.config['data_lake']['storage_account_name']

        command = f"""
        az storage account create \
            --name {storage_account} \
            --resource-group {self.resource_group} \
            --location {self.location} \
            --sku Standard_LRS \
            --kind StorageV2 \
            --hierarchical-namespace true \
            --enable-large-file-share
        """

        success, output = self.run_command(command)
        if success:
            print(f"✓ Storage account '{storage_account}' created")

            # Create containers
            for container in ['bronze', 'silver', 'gold']:
                command = f"""
                az storage container create \
                    --name {container} \
                    --account-name {storage_account} \
                    --auth-mode login
                """
                self.run_command(command)
                print(f"  ✓ Container '{container}' created")
        else:
            raise Exception("Failed to deploy storage account")

    def deploy_databricks_workspace(self):
        """Deploy Azure Databricks workspace"""
        print("\n" + "="*80)
        print("Deploying Databricks Workspace")
        print("="*80)

        workspace_name = self.config['databricks']['workspace_name']

        command = f"""
        az databricks workspace create \
            --resource-group {self.resource_group} \
            --name {workspace_name} \
            --location {self.location} \
            --sku premium
        """

        success, output = self.run_command(command)
        if success:
            print(f"✓ Databricks workspace '{workspace_name}' deployed")
        else:
            raise Exception("Failed to deploy Databricks workspace")

    def deploy_sql_database(self):
        """Deploy Azure SQL Database"""
        print("\n" + "="*80)
        print("Deploying Azure SQL Database")
        print("="*80)

        server_name = self.config['sql_database']['server_name']
        database_name = self.config['sql_database']['database_name']

        # Create SQL Server
        command = f"""
        az sql server create \
            --resource-group {self.resource_group} \
            --name {server_name} \
            --location {self.location} \
            --admin-user sqladmin \
            --admin-password $(openssl rand -base64 32)
        """

        success, output = self.run_command(command)
        if success:
            print(f"✓ SQL Server '{server_name}' created")

            # Enable Azure AD authentication
            command = f"""
            az sql server ad-admin create \
                --resource-group {self.resource_group} \
                --server-name {server_name} \
                --display-name DBAAdmin \
                --object-id $(az ad signed-in-user show --query id -o tsv)
            """
            self.run_command(command)
            print("✓ Azure AD authentication enabled")

            # Create database
            command = f"""
            az sql db create \
                --resource-group {self.resource_group} \
                --server {server_name} \
                --name {database_name} \
                --service-objective S3 \
                --zone-redundant false
            """
            success, output = self.run_command(command)
            if success:
                print(f"✓ Database '{database_name}' created")

            # Configure firewall
            command = f"""
            az sql server firewall-rule create \
                --resource-group {self.resource_group} \
                --server {server_name} \
                --name AllowAzureServices \
                --start-ip-address 0.0.0.0 \
                --end-ip-address 0.0.0.0
            """
            self.run_command(command)
            print("✓ Firewall configured")
        else:
            raise Exception("Failed to deploy SQL Server")

    def deploy_log_analytics(self):
        """Deploy Log Analytics workspace for monitoring"""
        print("\n" + "="*80)
        print("Deploying Log Analytics Workspace")
        print("="*80)

        workspace_name = self.config['monitoring']['log_analytics_workspace']

        command = f"""
        az monitor log-analytics workspace create \
            --resource-group {self.resource_group} \
            --workspace-name {workspace_name} \
            --location {self.location}
        """

        success, output = self.run_command(command)
        if success:
            print(f"✓ Log Analytics workspace '{workspace_name}' created")
        else:
            raise Exception("Failed to deploy Log Analytics")

    def configure_managed_identities(self):
        """Configure managed identities and permissions"""
        print("\n" + "="*80)
        print("Configuring Managed Identities")
        print("="*80)

        adf_name = self.config['data_factory']['name']
        storage_account = self.config['data_lake']['storage_account_name']

        # Get ADF managed identity
        command = f"""
        az datafactory show \
            --resource-group {self.resource_group} \
            --factory-name {adf_name} \
            --query identity.principalId \
            -o tsv
        """
        success, principal_id = self.run_command(command)

        if success and principal_id:
            principal_id = principal_id.strip()

            # Grant Storage Blob Data Contributor role
            command = f"""
            az role assignment create \
                --assignee {principal_id} \
                --role "Storage Blob Data Contributor" \
                --scope /subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.Storage/storageAccounts/{storage_account}
            """
            self.run_command(command)
            print("✓ ADF granted Storage Blob Data Contributor")

        print("✓ Managed identities configured")

    def deploy_all(self):
        """Deploy all infrastructure components"""
        try:
            print("\n" + "="*80)
            print("LIMS Migration - Infrastructure Deployment")
            print("="*80)

            self.login_azure()
            self.create_resource_group()
            self.deploy_data_factory()
            self.deploy_storage_account()
            self.deploy_databricks_workspace()
            self.deploy_sql_database()
            self.deploy_log_analytics()
            self.configure_managed_identities()

            print("\n" + "="*80)
            print("✓ Infrastructure deployment completed successfully!")
            print("="*80)

            return True

        except Exception as e:
            print(f"\n❌ Deployment failed: {str(e)}")
            return False


def main():
    """Main execution"""
    config_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "config",
        "azure_config.json"
    )

    if not os.path.exists(config_path):
        print(f"❌ Configuration file not found: {config_path}")
        sys.exit(1)

    deployer = InfrastructureDeployer(config_path)
    success = deployer.deploy_all()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
