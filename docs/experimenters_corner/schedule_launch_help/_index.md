This page lets you generate/update a crontab entry to push the "launch jobs" button for
this project on a schedule.

If there is a current crontab entry , it shows in the labelled box, otherwise it shows
"None"

To set times, fill in the lower form with cron values (see the [cron man page](https://man7.org/linux/man-pages/man5/crontab.5.html) for how to complicated things).

For example, to run it at 3:00 every morning, you would fill in 0 in Minutes, 0 in hours, and check All for the days of the week, and leave days of the month as an asterisk "*".

Or to run it on the first of every month, set fill in as above, but set the days of the month to "1".

Then hit "Submit", and the current crontab entry should update, and you have scheduled an entry.

To stop submissisions, the "Delete" button should clear the current entry to "None".