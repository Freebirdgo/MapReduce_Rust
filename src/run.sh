#!/bin/bash


set -e

# Function to clean up previous outputs
clean() {
    rm -f mr-*-*.txt
    rm -f *.nohup
    echo "Cleaned up previous output files."
}



# Function to generate the final output
generate_output() {
    rm -f mr-*-*.txt
    cat mr-* | sort > final.txt
    echo "Generated sorted final output."
}



# Main function to orchestrate tasks
main() {
    clean
   

    echo "Generate the final file."

    

    generate_output
}

# Execute the main function
main
