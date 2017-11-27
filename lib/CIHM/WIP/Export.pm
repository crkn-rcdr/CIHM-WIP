package CIHM::Meta::Export;

use strict;
use Carp;
use CIHM::WIP;
use CIHM::Meta::Export::Worker;
use Log::Log4perl;

use Coro::Semaphore;
use AnyEvent::Fork::Pool;

use Try::Tiny;
use JSON;
use Data::Dumper;

=head1 NAME

CIHM::Meta::Export - Exports AIPs,SIPs or metadata.xml files to directory in WIPbased on database documents in 'wipmeta'


=head1 SYNOPSIS

    my $export = CIHM::Meta::Export->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as defined in CIHM::WIP

=cut

sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::TDR::Replication->new() not a hash\n";
    };
    $self->{args} = $args;

    $self->{WIP} = new CIHM::WIP($self->configpath);

    Log::Log4perl->init_once("/etc/canadiana/wip/log4perl.conf");
    $self->{logger} = Log::Log4perl::get_logger("CIHM::Meta::Export");

    $self->{skip}=delete $args->{skip};

    $self->{maxprocs}=delete $args->{maxprocs};
    if (! $self->{maxprocs}) {
        $self->{maxprocs}=3;
    }

    # Set up for time limit
    $self->{timelimit} = delete $args->{timelimit};
    if($self->{timelimit}) {
        $self->{endtime} = time() + $self->{timelimit};
    }


    # Set up in-progress hash (Used to determine which AIPs which are being
    # processed by a slave so we don't try to do the same AIP twice.
    $self->{inprogress}={};

    $self->{limit}=delete $args->{limit};
    if (! $self->{limit}) {
        $self->{limit} = ($self->{maxprocs})*2+1
    }

    return $self;
}

# Simple accessors for now -- Do I want to Moo?
sub args {
    my $self = shift;
    return $self->{args};
}
sub configpath {
    my $self = shift;
    return $self->{args}->{configpath};
}
sub skip {
    my $self = shift;
    return $self->{skip};
}
sub maxprocs {
    my $self = shift;
    return $self->{maxprocs};
}
sub limit {
    my $self = shift;
    return $self->{limit};
}
sub endtime {
    my $self = shift;
    return $self->{endtime};
}
sub WIP {
    my $self = shift;
    return $self->{WIP};
}

sub config {
    my $self = shift;
    return $self->WIP->{config};
}
sub log {
    my $self = shift;
    return $self->{logger};
}
sub wipmeta {
    my $self = shift;
    return $self->WIP->{wipmeta};
}

sub export {
    my ($self) = @_;


    $self->log->info("Export: conf=".$self->configpath." skip=".$self->skip. " limit=".$self->limit. " maxprocs=" . $self->maxprocs . " timelimit=".$self->{timelimit});

    my $pool = AnyEvent::Fork
        ->new
        ->require ("CIHM::Meta::Export::Worker")
        ->AnyEvent::Fork::Pool::run 
        (
         "CIHM::Meta::Export::Worker::export",
         max        => $self->maxprocs,
         load       => 2,
         on_destroy => ( my $cv_finish = AE::cv ),
        );


    # Semaphore keeps us from filling the queue with too many AIPs before
    # some are processed.
    my $sem = new Coro::Semaphore ($self->maxprocs*2);
    while (my ($aip,$date) = $self->getNextAIP) {
        $self->{inprogress}->{$aip}=1;
        $sem->down;
        $pool->($aip,$date,$self->configpath,sub {
            my $aip=shift;
            $sem->up;
            delete $self->{inprogress}->{$aip};
                });
    }
    undef $pool;
    $cv_finish->recv;
}

sub getNextAIP {
    my $self = shift;

    return if $self->endtime && time() > $self->endtime;

    my $skipparam = '';
    if ($self->skip) {
        $skipparam="&skip=".$self->skip;
    }

    $self->wipmeta->type("application/json");
    my $res = $self->wipmeta->get("/".$self->wipmeta->{database}."/_design/tdr/_view/exportq?reduce=false&limit=".$self->limit.$skipparam,{}, {deserializer => 'application/json'});
    if ($res->code == 200) {
        if (exists $res->data->{rows}) {
            foreach my $hr (@{$res->data->{rows}}) {
                my $uid=$hr->{id};
                my $date=$hr->{key};
                if (! exists $self->{inprogress}->{$uid}) {
                    return ($uid,$date);
                }
            }
        }
    }
    else {
        warn "_view/exportq GET return code: ".$res->code."\n"; 
    }
    return;
}

1;
