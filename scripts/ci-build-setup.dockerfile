#----- --- -- -  -  -   -
# Extra User Commands to be run to set up a container-based ci build.
#

RUN which cor 2>&1 >/dev/null || {{ \
    pipx install cor-launcher --index-url https://gitlab.com/api/v4/projects/64628567/packages/pypi/simple ; \
    cor-setup ; \
}}

#----- --- -- -  -  -   -
# Initialize a clean Conan cache/home.
#
ENV CONAN_HOME "{user_home_dir}/_cache/.conan2"
RUN cor conan profile detect
