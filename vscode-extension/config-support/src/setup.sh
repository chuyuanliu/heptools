for lang in yaml json; do
    wget -O ${lang}.language-configuration.json \
    https://raw.githubusercontent.com/microsoft/vscode/refs/heads/main/extensions/${lang}/language-configuration.json 
done