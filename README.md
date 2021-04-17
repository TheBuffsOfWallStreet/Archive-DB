# Instructions to run

**Requires:**
docker,
docker-compose

1. Build docker containers

```
# database/
docker-compose up
```
This creates a mongo database container and a python runner container.

2. Run worker functions

Remember to configure password and authentication information.

```
# taskrunners/downloader
docker-compose up

# taskrunners/cleaner
docker-compose up
```

TODO: Run docker containers at regular intervals to keep database up to date.

# Contributors

**Developers:**
[Royce Schultz](https://github.com/royceschultz),
[Andrew Yee](https://github.com/AndrewYeeYee),
[Chace Trevino](https://github.com/chacetrev10),
[Tianwei Zhao](https://github.com/ZTWHHH)

**Project Sponsor:**
[Diego Garcia](https://www.colorado.edu/business/leeds-directory/faculty/diego-garcia),
[CU Boulder FINLAB](http://leeds-faculty.colorado.edu/AsafBernstein/NLP_FIN_LAB.html)

# License

[MIT Open Source License](LICENSE.txt)
