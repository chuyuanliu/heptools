# compile scss to css
cd _static/css
for input in *.scss; do
    if [[ $input != _* ]]; then
        output=${input%.scss}.css
        sass --style compressed --no-source-map $input $output
    fi
done