

def process_line(line):
    if 'index.markdown' in line:
        return None, None
        
    depth = line.count('│')
    name = line.split('── ')[-1].strip()
    return depth + 1, name

def generate_sidebar(tree_output):
    DIR=["docs"]
    sidebar_html = '<aside class="sidebar">\n  <nav class="toc">\n    <ul>\n'
    prev_depth = 0

    for line in tree_output:
        depth, name = process_line(line)
        if not depth and not name:
            continue
        elif depth > prev_depth:
            DIR.append(name)
            sidebar_html += '      ' * prev_depth + '<ul>\n'
        elif depth < prev_depth:
            print(name)
            DIR = DIR[:depth + 1]
            sidebar_html += '      ' * prev_depth + '</ul>\n'

        DIR[depth] = name
        print(DIR)
        sidebar_html += '      ' * depth + f'<li><a href="{{{{ site.url }}}}/{"/".join(DIR)}">{name.replace("_", " ").title()}</a></li>\n'
        prev_depth = depth
    sidebar_html += '      ' * prev_depth + '</ul>\n' * (prev_depth + 1)
    sidebar_html += '  </nav>\n</aside>'
    return sidebar_html

# Load tree command output
with open('tree.txt', 'r') as file:
    tree_output = file.readlines()

# Generate sidebar HTML
sidebar_html = generate_sidebar(tree_output)

# Save the sidebar HTML to a file
with open('sidebar.html', 'w') as file:
    file.write(sidebar_html)

print('Sidebar HTML has been generated and saved to sidebar.html.')
