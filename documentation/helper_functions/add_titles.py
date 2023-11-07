import os

# Define the path to the docs directory
docs_dir = '/Users/ltrestka/Desktop/src/poms_src/poms/documentation/docs'

def capitalize_title(file_name):
    # Replace underscores with spaces and capitalize each word
    title = ' '.join(word.capitalize() for word in file_name.replace('_', ' ').split())
    return title

def process_file(file_path, file_name):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Check if the file contains the specified text
    if f'---\ntitle:' not in content:
        # Get the capitalized title from the file name
        print(f"{file_path}/{file_name}")
        return
        title = capitalize_title(dir)
        # Create the new text to be added
        new_text = f'---\ntitle: {title}\n---\n'
        # Prepend the new text to the existing content
        updated_content = content.replace(f'---\ntitle: Index\n---\n', new_text)
        print(f"Updated title to {file_path}/{file_name}: {title}")
        # Write the updated content back to the file
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(updated_content)

# Walk through the docs directory and process each markdown file
for root, dirs, files in os.walk(docs_dir):
    for file in files:
        if file.endswith('.markdown') or file.endswith('.md'):
            # Exclude the extension from the file name when generating the title
            file_name_without_extension = os.path.splitext(file)[0]
            process_file(os.path.join(root, file), file_name_without_extension)

print("Files updated successfully.")
