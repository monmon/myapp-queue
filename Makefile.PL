use inc::Module::Install;
name 'MyApp-Queue';
all_from 'lib/MyApp/Queue.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
