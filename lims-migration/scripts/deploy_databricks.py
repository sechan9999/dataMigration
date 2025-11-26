#!/usr/bin/env python3
"""
LIMS Migration - Databricks Deployment Script
Deploys notebooks and configures Databricks workspace
"""

import json
import os
import sys
import subprocess
from pathlib import Path


class DatabricksDeployer:
    """Handles deployment of Databricks artifacts"""

    def __init__(self, config_path: str):
        """Initialize deployer with configuration"""
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        self.workspace_url = self.config['databricks']['workspace_url']
        self.notebooks_path = Path(__file__).parent.parent / 'databricks'

    def run_command(self, command: str) -> tuple:
        """Execute shell command"""
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

    def upload_notebooks(self):
        """Upload notebooks to Databricks workspace"""
        print("\n" + "="*80)
        print("Uploading Databricks Notebooks")
        print("="*80)

        # Notebook directories
        notebook_dirs = [
            'bronze_to_silver',
            'silver_to_gold',
            'validation',
            'ml'
        ]

        for dir_name in notebook_dirs:
            source_dir = self.notebooks_path / dir_name
            target_path = f"/Workspace/LIMS/{dir_name}"

            if not source_dir.exists():
                print(f"⚠ Directory not found: {source_dir}")
                continue

            # Create workspace directory
            command = f"""
            databricks workspace mkdirs {target_path}
            """
            self.run_command(command)

            # Upload notebooks
            for notebook_file in source_dir.glob('*.py'):
                target_file = f"{target_path}/{notebook_file.stem}"

                command = f"""
                databricks workspace import \
                    {notebook_file} \
                    {target_file} \
                    --language PYTHON \
                    --overwrite
                """
                success, output = self.run_command(command)

                if success:
                    print(f"  ✓ Uploaded: {notebook_file.name}")
                else:
                    print(f"  ❌ Failed: {notebook_file.name}")

        print("✓ Notebooks uploaded successfully")

    def create_cluster(self):
        """Create Databricks cluster"""
        print("\n" + "="*80)
        print("Creating Databricks Cluster")
        print("="*80)

        cluster_config = self.config['databricks']['cluster_config']

        config_json = {
            "cluster_name": cluster_config['cluster_name'],
            "spark_version": cluster_config['spark_version'],
            "node_type_id": cluster_config['node_type_id'],
            "autoscale": cluster_config['autoscale'],
            "spark_conf": cluster_config['spark_conf']
        }

        # Write config to temp file
        config_file = '/tmp/cluster_config.json'
        with open(config_file, 'w') as f:
            json.dump(config_json, f, indent=2)

        command = f"""
        databricks clusters create --json-file {config_file}
        """

        success, output = self.run_command(command)

        if success:
            print(f"✓ Cluster '{cluster_config['cluster_name']}' created")
            # Extract cluster ID from output
            cluster_id = json.loads(output).get('cluster_id')
            print(f"  Cluster ID: {cluster_id}")
        else:
            print("⚠ Cluster may already exist")

    def install_libraries(self):
        """Install required libraries on cluster"""
        print("\n" + "="*80)
        print("Installing Libraries")
        print("="*80)

        libraries = [
            {"pypi": {"package": "mlflow"}},
            {"pypi": {"package": "scikit-learn"}},
            {"pypi": {"package": "pandas"}},
            {"pypi": {"package": "numpy"}}
        ]

        # Get cluster ID
        cluster_name = self.config['databricks']['cluster_config']['cluster_name']
        command = f"""
        databricks clusters list --output JSON
        """

        success, output = self.run_command(command)

        if success:
            clusters = json.loads(output).get('clusters', [])
            cluster = next((c for c in clusters if c['cluster_name'] == cluster_name), None)

            if cluster:
                cluster_id = cluster['cluster_id']

                for lib in libraries:
                    lib_json = json.dumps({"cluster_id": cluster_id, "libraries": [lib]})
                    command = f"""
                    echo '{lib_json}' | databricks libraries install --json-stdin
                    """
                    self.run_command(command)
                    print(f"  ✓ Installed: {lib}")

                print("✓ Libraries installed successfully")
            else:
                print("⚠ Cluster not found")

    def configure_secrets(self):
        """Configure Databricks secrets"""
        print("\n" + "="*80)
        print("Configuring Secrets")
        print("="*80)

        # Create secret scope
        scope_name = "lims-secrets"
        command = f"""
        databricks secrets create-scope --scope {scope_name}
        """
        self.run_command(command)
        print(f"✓ Secret scope '{scope_name}' created")

    def deploy_all(self):
        """Deploy all Databricks components"""
        try:
            print("\n" + "="*80)
            print("LIMS Migration - Databricks Deployment")
            print("="*80)

            self.upload_notebooks()
            self.create_cluster()
            self.install_libraries()
            self.configure_secrets()

            print("\n" + "="*80)
            print("✓ Databricks deployment completed successfully!")
            print("="*80)

            return True

        except Exception as e:
            print(f"\n❌ Deployment failed: {str(e)}")
            return False


def main():
    """Main execution"""
    config_path = Path(__file__).parent.parent / 'config' / 'azure_config.json'

    if not config_path.exists():
        print(f"❌ Configuration file not found: {config_path}")
        sys.exit(1)

    deployer = DatabricksDeployer(str(config_path))
    success = deployer.deploy_all()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
