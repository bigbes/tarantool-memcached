add_custom_target(test
        COMMAND ${PROJECT_SOURCE_DIR}/test/test-run.py)
add_dependencies(test parser)
