# AWS Glue Jobs Python Local Develop with Container Setup Guide

## Prerequisites
- Docker installed in your system.
    - In mac, I was able to run using `Colima` to run the Docker Engine and `Lazydocker` to instead of Licensed `Docker Desktop`
- Visual Studio Code installed on your machine.
- Remote – Containers extension for VSCode. You can install it by searching 'Remote – Containers' in the Extensions view within Visual Studio Code.
- Python extension

## Installation and Running Instructions
1. Pull (download) the AWS Glue V4 Docker Image. For other versions check https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html  
`docker pull amazon/aws-glue-libs:glue_libs_4.0.0_image_01`
1. Clone your project repository to your local machine using: 
```bash
git clone <this-repo-url>
```
1. Open VSCode, click on Remote-Containers in the left sidebar or press Ctrl+Shift+P and type 'Remote-Containers: Open Folder in Container'. Select your cloned project folder.
1. If this is the first time you're opening a container with this folder, VSCode will build the container based on the Dockerfile found at .devcontainer.json in the root of your workspace. This process may take some time as it needs to download and install all dependencies specified in the Dockerfile.
1. Install python extension inside VSCode Container
1. Once the container is built, you can start coding inside VSCode.
1. If the linter cannot reference and or python cannot import, copy awsglue to your workspace. TODO
1. If you want to run a specific script or command within this container, open Terminal in VSCode (`Ctrl+``) and type your command.
1. To stop the running container, simply close the terminal window.
