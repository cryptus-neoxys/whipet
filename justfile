# Thin wrapper over the Makefile. Keep logic in scripts/ and the Makefile.
set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

up:
    make up

down:
    make down

demo:
    make demo

bench:
    make bench

bench-sweep:
    make bench-sweep

verify:
    make verify

replay:
    make replay

doctor:
    make doctor

reset:
    make reset

logs:
    make logs

build:
    make build

dashboard:
    make dashboard

ci:
    make ci

obs:
    make obs

chaos:
    make chaos

ha:
    make ha
