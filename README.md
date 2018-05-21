# crepes
This is the GitHub repository for the Python package called **crepes** (Clinical Research Engine for Profile Extraction & Summarization) can be used to create the files needed for submission to the clinical profiles registry [here](https://github.com/NCATS-Tangerine/clinical-profile-registry/blob/master/README.md).

## License
By committing your code to the crepes code repository you agree to release the code under the [MIT License](https://github.com/translational-informatics/crepes/blob/master/LICENSE) attached to the repository.

## Acknowledgements
This work as part of the Biomedical Translator Program was initiated and funded by NCATS (NIH awards 1OT3TR002019, 1OT3TR002020, 1OT3TR002025, 1OT3TR002026, 1OT3TR002027, 1OT2TR002514, 1OT2TR002515, 1OT2TR002517, 1OT2TR002520, 1OT2TR002584). Any opinions expressed in this document are those of the Translator community and do not necessarily reflect the views of NCATS, individual Translator team members, or affiliated organizations and institutions.

## Installation and Use
A prototype package is now available.  The package has a default configuration
which reads one record from a Synpuf loaded omop database and then generate a
profile.

Installation

    pip install crepes

Usage:

    python
    import crepes
    >>> patients = crepes.read_omop_data()
    >>> crepes.generate(patients)

Here are some parameters that can be passed to change the behavior:

    read_omop_data(npatients=1,
                   dbname='ohdsi', # PostgreSQL database name
                   user='synpuf',  # PostgreSQL user
                   host='synpuf.c6pyjs3halpn.us-west-2.rds.amazonaws.com',
                   password='...', # Password
                   db_prefix='synpuf5.') # Prefix for tables
    generate(patients,{ configuration-parameters })

There are many parameters.  See the source code.  For now, they are not
completely tested so use at your own risk.

