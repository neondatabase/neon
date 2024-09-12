#!/bin/bash


cargo neon endpoint stop main
cargo neon endpoint start main --create-test-user true
