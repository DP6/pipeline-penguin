#!/bin/bash
echo "Executando $0 com os parâmetros versão=$1 e bucket=$2"
echo "Acessando diretorio raiz do projeto"
cd ../src
echo "$(pwd)"
echo "Criando zip"
zip -r "../$1.zip" *
cd ..
echo "Movendo Zip para terraform/files-copy-to-gcs/pipeline-penguin/"
mv "$1.zip" ./terraform/files-copy-to-gcs/pipeline-penguin/
echo "Entrando nas pasta terraform para copiar os arquivos"
cd terraform
echo "Iniciando copia para GCP"
gsutil cp -r "./files-copy-to-gcs/pipeline-penguin/$1.zip" "gs://$2"
echo "excluindo zip"
rm -rf "./files-copy-to-gcs/pipeline-penguin/$1.zip"
echo "FIM script $0"