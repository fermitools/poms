---
layout: page
title: Namespaces
---
* TOC
{:toc}
In the common situation that we are using POMS in conjunction with fife_launch, we have two namespaces which both use the %(name)s syntax to provide the value associated with the name: Poms launch commands and fife_launch config files. This wiki page will discuss them both, and how they're related.

The first such namespace is in POMS for launch commands; where there is a namespace including the dataset POMS thinks it should use, the experiment, the experimenter (or username), the software version, and group, along with any campaign keywords you defined in the GUI. This means that in POMS launch commands, including the option-value pairs you pass in, the %(name)s syntax gets you those values. This means you can put your launch command text things that make a launch command:  
my_launch_command --foo=%(dataset)s --bar=%(fred)s
and the %(dataset)s will be replaced with the POMS dataset, and if you have "fred" defined in your campaign keywoards, you'll get that value replaced for %(fred)s.

The second such namespace is in the fife_launch config file, which is an .ini file, where %(name)s gives you the value associated with "name" either in the current [blockname] section, or from the [global] block. THat is to say if you have

    [global]
    fred=hello
    [submit]
    schema=%(fred)s

you'll get "hello" for the schema.

These two namespaces would be completely decoupled, except that fife_launch also accepts the -O flag, which lets you override the values of globals from the launch command line. So now I can run "fife_launch -Oglobal.fred=goodbye" and the config file above, and I'll get "goodbye" for the schema. This means that in POMS if I'm running fife_launch, I can do:

    fife_launch -Oglobal.fred=%(experiment)s

and pass the POMS experiment into the fife_launch config. I can even do

    fife_launch -Oglobal.fred=%(fred)s

and pass the fife campaign keyword "fred" value in as the fife_launch globals "fred". This doesn't happen automatically, it only happens if you explicitly add a -O flag to pass it through.

So is it good to use the same name in both namespaces, as in the last example above? I could argue it either way.  
Functionally it doesn't make any difference, but calling the fife_launch global something like "poms_fred" might be clearer...
I think the more important part is actually to mention the names in the fife_launch config file [global] with some flag value, like  
"override_me" to make it clear we want this value to be overridden on the command line, then if you see an "override_me"  
in your actual launch command, you know someone forgot a -O flag to pass in the value. If you make those flag values override_me_1, override_me_2, etc. you can even tell more easily which one was omitted on the command line.