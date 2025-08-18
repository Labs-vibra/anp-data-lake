#!/usr/bin/env python3

import os
import subprocess
import sys
import time
from docker_images_config import DOCKER_IMAGES, ARTIFACT_REGISTRY_BASE_URL, get_image_by_name

def show_usage():
    """Display usage instructions and available images"""
    print("🐳 Docker Image Upload Tool")
    print("=" * 50)
    print("Usage: python upload-single-docker-image.py <image_name>")
    print("\n📋 Available images:")
    for img in DOCKER_IMAGES:
        print(f"  - {img['name']}")
        print(f"    Label: {img['label']}")
        print(f"    Path: {img['path']}")
        print()

def build_and_push_image(image):
    """Build and push a single Docker image"""
    image_name = image['name']
    image_path = image['path']
    image_label = image['label']

    print(f"🏗️  Building and pushing: {image_label}")
    print(f"📁 Path: {image_path}")
    print(f"🏷️  Tag: {ARTIFACT_REGISTRY_BASE_URL}{image_name}")
    print("-" * 60)

    # Build command
    build_command = [
        "docker", "build",
        "--platform", "linux/amd64",
        "-t", f"{ARTIFACT_REGISTRY_BASE_URL}{image_name}",
        image_path
    ]

    # Push command
    push_command = [
        "docker", "push",
        f"{ARTIFACT_REGISTRY_BASE_URL}{image_name}"
    ]

    try:
        # Build image
        print("🔨 Building Docker image...")
        build_process = subprocess.run(
            build_command,
            capture_output=False,
            text=True,
            check=True
        )
        print("✅ Build completed successfully!")

        # Push image
        print("📤 Pushing Docker image...")
        push_process = subprocess.run(
            push_command,
            capture_output=False,
            text=True,
            check=True
        )
        print("✅ Push completed successfully!")
        print(f"🎉 {image_label} deployed successfully!")

        return True

    except subprocess.CalledProcessError as e:
        print(f"❌ Error: Command failed with return code {e.returncode}")
        print(f"Command: {' '.join(e.cmd)}")
        return False

def main():
    if len(sys.argv) != 2:
        print("❌ Error: Please provide exactly one image name as argument")
        print()
        show_usage()
        sys.exit(1)

    image_name = sys.argv[1]

    print(f"🔍 Searching for image: {image_name}")

    # Handle help requests
    if image_name in ["-h", "--help", "help"]:
        show_usage()
        sys.exit(0)

    # Find the image using the centralized function
    image = get_image_by_name(image_name)
    if not image:
        print(f"❌ Error: Image '{image_name}' not found!")
        print()
        show_usage()
        sys.exit(1)

    # Check if the path exists
    if not os.path.exists(image['path']):
        print(f"❌ Error: Path '{image['path']}' does not exist!")
        sys.exit(1)

    # Check if Dockerfile exists
    dockerfile_path = os.path.join(image['path'], 'Dockerfile')
    if not os.path.exists(dockerfile_path):
        print(f"❌ Error: Dockerfile not found at '{dockerfile_path}'!")
        sys.exit(1)

    # Build and push the image
    success = build_and_push_image(image)

    if success:
        print("\n🏁 Process completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Process failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
