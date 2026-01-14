#!/usr/bin/env python3
"""
Merge separate launch.json files from component directories into main launch.json

This script reads launch.json files from:
- .vscode/fundamentals/launch.json (EDGAR)
- .vscode/macro_model/fred/launch.json (FRED)
- .vscode/macro_model/bls/launch.json (BLS)
- .vscode/macro_model/eurostat/launch.json (Eurostat)
- .vscode/macro_model/imf/launch.json (IMF)
- .vscode/model/hmm/launch.json (HMM Model)

And merges them into .vscode/launch.json
"""

import json
import os
from pathlib import Path

def load_launch_config(file_path):
    """Load launch.json configuration from a file"""
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)
    return None

def merge_launch_configs():
    """Merge all component launch.json files into main launch.json"""
    # Get workspace root (parent of .vscode directory)
    script_path = Path(__file__).resolve()
    vscode_dir = script_path.parent
    workspace_root = vscode_dir.parent
    main_launch = vscode_dir / 'launch.json'
    
    # Component launch.json files
    component_files = [
        vscode_dir / 'fundamentals' / 'launch.json',
        vscode_dir / 'macro_model' / 'fred' / 'launch.json',
        vscode_dir / 'macro_model' / 'bls' / 'launch.json',
        vscode_dir / 'macro_model' / 'eurostat' / 'launch.json',
        vscode_dir / 'macro_model' / 'imf' / 'launch.json',
        vscode_dir / 'macro_model' / 'bis' / 'launch.json',
        vscode_dir / 'model' / 'hmm' / 'launch.json',
    ]
    
    # Collect all configurations
    all_configs = []
    
    for component_file in component_files:
        config = load_launch_config(component_file)
        if config and 'configurations' in config:
            all_configs.extend(config['configurations'])
            print(f"Loaded {len(config['configurations'])} configurations from {component_file.relative_to(workspace_root)}")
    
    # Create merged launch.json
    merged_config = {
        "version": "0.2.0",
        "configurations": all_configs
    }
    
    # Write merged config to main launch.json
    with open(main_launch, 'w') as f:
        json.dump(merged_config, f, indent=4)
    
    print(f"\n✓ Merged {len(all_configs)} configurations into {main_launch.relative_to(workspace_root)}")
    return len(all_configs)

if __name__ == "__main__":
    try:
        count = merge_launch_configs()
        print(f"\nSuccess! {count} total launch configurations available.")
    except Exception as e:
        print(f"Error merging launch configurations: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
