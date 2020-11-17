FILE=/singularity/runner.simg
if [ -f "$FILE" ]; then
    echo "$FILE exists. Skipping build."
else
    # $FILE does not exist
    singularity build $FILE /singularity/runner.Singularityfile
fi

FILE=/singularity/database.simg
if [ -f "$FILE" ]; then
    echo "$FILE exists. Skipping build."
else
    # $FILE does not exist
    singularity build $FILE /singularity/database.Singularityfile
fi
