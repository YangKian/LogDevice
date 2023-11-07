FROM ocaml/opam:ubuntu-22.04-ocaml-4.14

USER root

ENV PATH="/home/opam/.opam/4.14/bin:${PATH}"

COPY docker/build_deps/ubuntu.deps /deps/ubuntu.deps
RUN apt-get update && \
    DEBIAN_FRONTEND="noninteractive" apt-get install --no-install-recommends \
      -y $(cat /deps/ubuntu.deps) ninja-build gcc-10 g++-10 && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 20 && \
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-10 20 && \
    rm -rf /var/lib/apt/lists/*

RUN opam install dune yojson ppx_deriving_yojson
RUN git clone https://github.com/daleiz/mbuild.git && \
    opam pin add -y mbuild mbuild && \
    rm -rf mbuild

COPY logdevice/ /LogDevice/logdevice 
COPY build/     /LogDevice/build/
COPY common/    /LogDevice/common/
RUN cd /LogDevice/logdevice && dune exec ./build.exe && ninja

# vim: set ft=dockerfile:
