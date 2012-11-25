use strict;
use warnings;
use Test::More;

use DBI;
use JSON::Syck;
use MyApp::Queue;
use Data::Dumper;

subtest 'new' => sub {
    my $dbh = DBI->connect('dbi:mysql:dbname=test;host=localhost;port=8889;mysql_socket=/Applications/MAMP/tmp/mysql/mysql.sock', 'root', 'root');
    my $qname = 'myqueue';
    my $queue = MyApp::Queue->new($dbh, $qname);
    
    is $queue->{dbh}, $dbh, 'dbh';
    is $queue->{qname}, $qname, 'qname';
};

subtest 'queue' => sub {
    my($dbh, $qname) = setup();

    my $queue = MyApp::Queue->new($dbh, $qname);
    
    my $expect_worker = 'MyApp::Worker::Test';
    my $expect_arg = {message => 'message'};
    $queue->enqueue($expect_worker, $expect_arg);
    my $row = $queue->dequeue;

    is $row->{worker}, $expect_worker, 'worker';
    is_deeply JSON::Syck::Load($row->{data}), $expect_arg, 'arg';

    teardown($dbh, $qname);
};

subtest 'no message in queue' => sub {
    my($dbh, $qname) = setup();

    my $queue = MyApp::Queue->new($dbh, $qname);
    
    ok !$queue->dequeue, 'no message';

    teardown($dbh, $qname);
};

subtest 'locked message in queue' => sub {
    my($dbh, $qname) = setup();

    my $queue = MyApp::Queue->new($dbh, $qname);
    
    my $worker = 'MyApp::Worker::Test';
    my $arg = {message => 'message'};
    my $sth = $dbh->prepare(qq|
        INSERT INTO `$qname` (worker, data, locked_until)
            VALUES (?, ?, NOW() + INTERVAL $MyApp::Queue::LOCK_TIMEOUT SECOND);
    |);
    $sth->execute($worker, JSON::Syck::Dump($arg));

    ok !$queue->dequeue, 'locked message';

    teardown($dbh, $qname);
};

sub setup {
    my $dbh = DBI->connect(
        'dbi:mysql:dbname=test;host=localhost;port=8889;mysql_socket=/Applications/MAMP/tmp/mysql/mysql.sock',
        'root',
        'root',
        {
            PrintError => 0,
            RaiseError => 1,
            AutoCommit => 0,
        }
    );
    my $qname = 'myqueue';

    my $create_qname = qq{
        CREATE TABLE `$qname` (
            `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            `locked_until` TIMESTAMP NOT NULL DEFAULT "0000-00-00 00:00:00",
            `worker`       VARCHAR(255) NOT NULL,
            `data`         BLOB NOT NULL,
            PRIMARY KEY  (`id`)
        ) ENGINE=InnoDB;
    };
    eval {
        $dbh->do($create_qname);
    };

    ($dbh, $qname);
}

sub teardown {
    my($dbh, $qname) = @_;

    $dbh->do("DROP TABLE $qname");
    $dbh->disconnect;
}

done_testing;
