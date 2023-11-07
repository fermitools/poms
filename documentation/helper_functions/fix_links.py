import os
import re

# Define the path to the docs directory
docs_dir = '/Users/ltrestka/Desktop/src/poms_src/poms/documentation/docs'

# Define a regex pattern to match the links
# This pattern now ignores the content inside the square brackets
pattern = re.compile(r'\[(.*?)\]\(\{\{ site\.url \}\}/docs\/(.*?)\)')

def find_and_replace(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Find all matches of the pattern
    matches = pattern.findall(content)
    
    for match in matches:
        # Find the relative path to the referenced directory
        link_text, link_target = match
        #print(match)
        
        for root, dirs, files in os.walk(docs_dir):
            if link_target in dirs:
                rel_path = os.path.relpath(os.path.join(root, link_target), docs_dir)
                current_link = "[%s]({{ site.url }}/docs/%s)" % (link_text, link_target)
                new_link = "[%s]({{ site.url }}/docs/%s)" % (link_text, rel_path)
                new_link = new_link.replace("/..", "")
                content = content.replace("[%s]({{ site.url }}/docs/%s)" % (link_text, link_target), new_link)
                if current_link != new_link:
                    print("Replaced: ")
                    print("[%s]({{ site.url }}/docs/%s)" % (link_text, link_target))
                    print(new_link)
                    print("\n")

    # Write the updated content back to the file
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(content)

# Walk through the docs directory and process each markdown file
for root, dirs, files in os.walk(docs_dir):
    for file in files:
        if file.endswith('.markdown') or file.endswith('.md'):
            find_and_replace(os.path.join(root, file))

print("Links updated successfully.")
