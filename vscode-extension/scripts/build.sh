license=$(readlink -f "$(dirname "$0")/../../LICENSE")
initialize() {
    cp "$license" "LICENSE"
}

convert_tmLanguage_yaml() {
    find src -type f -name "*.tmLanguage.yml" | while read -r source; do
        target="${source%.yml}.json"
        target="${target#src/}"
        mkdir -p "$(dirname "$target")"
        npx js-yaml "$source" > "$target"
    done
}

run_setup_script() {
    if [ -f "src/setup.sh" ]; then
        bash src/setup.sh
    fi
}

if [ "$#" -gt 0 ]; then
    extensions=("$@")
else
    extensions=()
    for extension in */; do
        if [ -f "$extension/package.json" ]; then
            extensions+=("$extension")
        fi
    done
fi
echo "Found extensions: ${extensions[@]}"

mkdir -p dist
for extension in "${extensions[@]}"; do
    echo "Building: $extension"
    cd "$extension"
    initialize
    convert_tmLanguage_yaml
    run_setup_script
    npx @vscode/vsce package
    mv *.vsix ../dist/
    cd ..
done


