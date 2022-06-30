#!/bin/bash
cd terraform && rm -rf .terraform && rm -rf .tfstate && terraform init && terraform apply -auto-approve