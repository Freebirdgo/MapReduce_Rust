#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

clean_all() {
    rm -f mr-*.txt
    rm -f *.nohup
    rm -f final.txt
    echo "Cleaned up all output files."
}

main() {
    clean_all
}

# Execute the main function
main