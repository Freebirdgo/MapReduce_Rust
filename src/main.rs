use std::fs::File;
use std::io::{self, Read};
use std::env;
fn main() -> Result<(), io::Error>{

    let file_name = "data/gut-0.txt";

    // Attempt to open the file
    let mut file = match File::open(file_name) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Error opening file {}: {}", file_name, e);
            return Err(e);
        }
    };

    // Create a string buffer to hold the file contents
    let mut contents = String::new();

    // Read the file contents into the string buffer
    match file.read_to_string(&mut contents) {
        Ok(_) => println!("File contents:\n{}", contents),
        Err(e) => {
            eprintln!("Error reading the file: {}", e);
            return Err(e);
        }
    }
    println!("Current working directory: {:?}", env::current_dir().unwrap());


    Ok(())

}