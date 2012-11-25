package MyApp::Queue;
use strict;
use warnings;
use JSON::Syck;
our $VERSION = '0.01';

our $LOCK_TIMEOUT = 10;

sub new {
    my $class = shift;
    my($dbh, $qname) = @_;

    bless {
        dbh   => $dbh,
        qname => $qname,
    }, $class;
}

sub enqueue {
    my $self = shift;
    my $worker = shift;
    my $arg = shift;

    my $sth = $self->{dbh}->prepare(qq|
        INSERT INTO `$self->{qname}` (worker, data)
            VALUES (?, ?);
    |);
    $sth->execute($worker, JSON::Syck::Dump($arg));
}

sub dequeue {
    my $self = shift;

    # 1. Lock the top message in the queue.
    #    Note that the lock will expire in $LOCK_TIMEOUT.
    my $affected = $self->{dbh}->do(qq|
        UPDATE `$self->{qname}`
            SET id = LAST_INSERT_ID(id),
                locked_until = NOW() + INTERVAL $LOCK_TIMEOUT SECOND
            WHERE locked_until < NOW() ORDER BY id LIMIT 1
    |);
    if ($affected == 0) {
        # No message in the queue.
        return;
    }

    # 2. Get the ID of the locked message.
    my $msg_id = $self->{dbh}->{mysql_insertid};
    if (!$msg_id) {
        # Oops, no message in the queue, or failed to lock a message maybe.
        return;
    }

    # 3. Get the data of the locked message.
    my $sth_select = $self->{dbh}->prepare(qq|
        SELECT worker, data FROM `$self->{qname}` WHERE id = ?
    |);
    if (!$sth_select->execute($msg_id)) {
        # In this case, we locked the message but have failed to get the data.
        # The message will stay in the queue for 10 secs, and another client will process it.
        return;
    }
    my $row = $sth_select->fetchrow_hashref;

    # 4. Delete the locked message from the queue.
    my $sth_delete = $self->{dbh}->prepare(qq|
        DELETE FROM `$self->{qname}` WHERE id = ?
    |);
    if (!$sth_delete->execute($msg_id)) {
        # In self case, we have the message data but the message stays in the queue,
        # so here we ignore the message data so that another client will receive it.
        return;
    }

    # And we're done!
    return $row;
}

1;
__END__

=head1 NAME

MyApp::Queue - Queue System

=head1 SYNOPSIS

  use MyApp::Queue;

=head1 DESCRIPTION

MyApp::Queue is queue system copied MyQueue.

refs. https://github.com/kotas/myqueue

=head1 AUTHOR

monmon E<lt>noE<gt>

=head1 SEE ALSO

https://github.com/kotas/myqueue

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
