---
layout: page
title: "GUI Campaign Editor"
---
* TOC
{:toc}

Current status -- new implementation based on Proposed Design (below) is up for more or less beta testing.
It might still want an extra box at the side to show the input dataset and split/partition type (if any)
for the first stage of the campaign. Working on a rough outline for a GUI Campaign Editor User Guide.

### Design Plan

My plan at the moment is to have the GUI Campaign Editor be basically a
small Javascript app that works in the browser window.

It starts up and fetches a json version of the .ini file dump of the campaign (if any) or provides a list of templates to choose from.

Each stage in the campaign as defined in order in the campaign stage list is laid out as a table column,
with a hidden popup form to edit the stage parameters.

If multiple stages depend on the same preceding stage, they should step down the rows of the table rather than accross columns.

There should be a "defaults" box that has campaign def fields with values that each stage will
default to if not filled in. This is, however, a feature of the editor -- we will keep a _defaults_tag_name
campaign stage with those values in the database, but only the editor (and the .ini dumper?) will use it.

Operations are:

* add new stage (on empty cells) (initializes with defaults, prompts for dependent, file subset params)
* start edit stage -- makes form for stage params visible, with params filled in
* close edit stage -- saves fields, hides form
* Delete stage -- deletes stage, drops box from table
* Change Dependency -- (prompts for dependent, file subset params)
* Download -- downloads ini file of campaign
* insert row/column -- gives layout room
* (?) drag stage to new location (layout)
* Save Campaign -- closes any open edit stages, saves all stages to database


### First prototype

First prototype was a really rough draft in mixed HTML/javascript which let us make boxes for campaign stages, and one for default campaign stage parameters, and have them be connected to show the campaign.

This lead me to the following design.

### Proposed Design

Editor:

* keeps editor state in a dictionary of dictionaries which represents the .ini-file format campaign dump
  * except: we pull out a "defaults" section
  * special representation of values that are default instead of empty(null vs '' maybe)
    initially populates the editor state with an .ini-format dump of a campaign
  * parser for this is already done
    * converts the .ini format to JSON
    * unpacks the JSON
* Each section in the .ini file is represented by a box-with-popup
* Three subclasses
  * generic box-with-popup
    * Used for
      * Launch(Login) Templates
      * Job types
    * Can be selected/unselected (change in border)
    * May have a secondary pop-up for parameter sets like the main POMS gui
      * could be shared with the main POMS gui(?)
    * box has a button that toggles state:
      * pops up a form
        * with input fields for the items in the section
        * filled with current values
      * pops down the form, and stores the fields back into the editor state
    * popup also has a delete button to delete the item from the state dictionary
  * Campaign State box-with-popup
    * subclass of generic one
    * draggable around the screen campaign area
      * drag code is partially prototyped (cargo culted from w3schools)
  * Dependency box
     * connects two states
    * placed so said states are at corners of box
    * redrawn whenever we drag a state
    * only modifiable params are file-pattern
    * drawing two borders makes dependency lines
* Aside from those, we need buttons to
  * connect selected campaign states with dependencies
  * create new
    * states
    * launch(login)templates
    * job types
  * save state back to POMS
    * can mimic logic in poms_client
