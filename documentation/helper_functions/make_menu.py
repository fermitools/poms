import os
import yaml
last_line = ""
last_entry = {"title": "", "url":""}
def generate_menu_yml(folder_path, indent=0, indir=False):
    menu_yml = ""
    indent_str = "  " * indent

    # Loop through each item in the folder

    for item in sorted(os.listdir(folder_path)):
        item_path = os.path.join(folder_path, item)
        
        # Skip hidden files and folders
        if item.startswith("."):
            continue
        
        # If it's a folder, recursively get its contents
        if os.path.isdir(item_path):
            path = "%s/index.html" % item_path.replace("/Users/ltrestka/Desktop/src/poms_src/poms/documentation", "http://localhost:4000")
            splitpath = path.split("/")
            filename = splitpath[len(splitpath) -2 ].replace("_", " ")
            title = ' '.join(word.capitalize() for word in filename.split())
            if not (last_entry["title"] == title or last_entry["url"] == path):
                print(f"{last_entry['title']} != {title}")
                print(f"{last_entry['url']} != {path}")
                last_entry["title"] = title
                last_entry["url"] = path
                menu_yml += f"{indent_str}- title: {title}\n"
                menu_yml += f"{indent_str}  url: {path}\n"
                if not indir:
                    menu_yml += f"{indent_str}  children:\n"
                menu_yml += generate_menu_yml(item_path, indent + 2, indir=True)
        # If it's a file, add it directly
        elif os.path.isfile(item_path):
            path = item_path.replace("/Users/ltrestka/Desktop/src/poms_src/poms/documentation", "http://localhost:4000").replace(".markdown", ".html")
            splitpath = item_path.split("/")
            filename = splitpath[len(splitpath) -2 ].replace("_", " ")
            title = ' '.join(word.capitalize() for word in filename.split())
            if not (last_entry["title"] == title or last_entry["url"] == path):
                print(f"{last_entry['title']} != {title}")
                print(f"{last_entry['url']} != {path}")
                last_entry["title"] = title
                last_entry["url"] = path
                menu_yml += f"{indent_str}- title: {title}\n"
                menu_yml += f"{indent_str}  url: {path}\n"
            
    return menu_yml

def clean_redundancies(yaml_dict):
    new_dict = []
    for entry in yaml_dict:
        if "children" in entry:
            entry["children"] = clean_redundancies(entry["children"])
            entry["children"] = [child for child in entry["children"] if not (child.get('title') == 'index' and child.get('url') == entry.get('url'))]
        new_dict.append(entry)
    return new_dict


yaml_data = generate_menu_yml('/Users/ltrestka/Desktop/src/poms_src/poms/documentation/docs')
#print(yaml_data)
#yaml_dict = yaml.safe_load(yaml_data)
#cleaned_yaml_dict = clean_redundancies(yaml_dict)
#cleaned_yaml_data = yaml.safe_dump(cleaned_yaml_dict, default_flow_style=False)
#print(cleaned_yaml_data)
os.system(f"echo '{yaml_data}' > documentation/helper_functions/menu.yml")
