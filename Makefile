UNIT_TESTS = $(shell find test -name '*.test.js')

SRC_FILES = $(shell find lib -type f \( -name "*.js" ! -path "*node_modules*" \))
JSHINT_CONFIG = .jshintrc

MOCHA = ./node_modules/.bin/mocha
UNIT_TEST_OPTIONS = --reporter spec -t 5000 --globals encoding --bail
BIN = ./node_modules/.bin

test-unit: jshint
	@NODE_ENV=testing $(MOCHA) $(UNIT_TEST_OPTIONS) $(UNIT_TESTS)


test-all:	test
test: test-unit


# PHONIES
.PHONY: test test-unit test-all

###
# Linting
###
jshint:
	@$(BIN)/jshint --config $(JSHINT_CONFIG) \
		$(SRC_FILES)


###
# Cleanup
###
clean:
	rm -rf build

clean-all: clean
	rm -rf node_modules

