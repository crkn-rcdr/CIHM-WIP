package CIHM::WIP::Ingest;

use strict;
use Carp;
use CIHM::TDR::TDRConfig;
use CIHM::TDR::Repository;
use CIHM::TDR::REST::wipmeta;
use CIHM::WIP::Ingest::Worker;

use Coro::Semaphore;
use AnyEvent::Fork;
use AnyEvent::Fork::Pool;

use Try::Tiny;
use JSON;
use Data::Dumper;

=head1 NAME

CIHM::WIP::Ingest - Ingest SIPs into an AIP based on database documents in 'wipmeta'


=head1 SYNOPSIS

    my $hammer = CIHM::WIP::Ingest->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as defined in CIHM::TDR::TDRConfig

=cut

sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::WIP::Ingest->new() not a hash\n";
    };
    $self->{args} = $args;

    $self->{config} = CIHM::TDR::TDRConfig->instance($self->configpath);
    $self->{logger} = $self->{config}->logger;


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

    # Confirm there is a named repository block in the config
    my %confighash = %{$self->{config}->get_conf};


    # Undefined if no <ingest> config block
    if (exists $confighash{ingest}) {
        if (exists $confighash{ingest}{tempdir}) {
            my $tempdir = $confighash{ingest}{tempdir};
            if (! -d $tempdir || ! -w $tempdir) {
                die "Path $tempdir is not a directory or not writeable\n";
            }
        } else {
            die "Missing tempdir option in <ingest> configuration block\n";
        }
        if (!exists $confighash{ingest}{outbox}) {
            die "Missing outbox option in <ingest> configuration block\n";
        }
    } else {
        die "Missing <ingest> configuration block\n";
    }


    # Undefined if no <wipmeta> config block
    if (exists $confighash{wipmeta}) {
        $self->{wipmeta} = new CIHM::TDR::REST::wipmeta (
            server => $confighash{wipmeta}{server},
            database => $confighash{wipmeta}{database},
            type   => 'application/json',
            conf   => $args->{configpath},
            clientattrs => {timeout => 3600},
            );
    } else {
        die "Missing <wipmeta> configuration block in config\n";
    }


    # Check things that workers need, but that parent doesn't.
    if (! new CIHM::TDR::Repository({
        configpath => $self->configpath
                                                     })) {
        die "Wasn't able to build Repository object\n";
    }
    # Undefined if no <internalmeta> config block
    if (! exists $confighash{tdrepo}) {
        die "Missing <tdrepo> configuration block in config\n";
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
sub config {
    my $self = shift;
    return $self->{config};
}
sub log {
    my $self = shift;
    return $self->{logger};
}
sub wipmeta {
    my $self = shift;
    return $self->{wipmeta};
}

sub ingest {
    my ($self) = @_;


    $self->log->info("Ingest: conf=".$self->configpath." skip=".$self->skip. " limit=".$self->limit. " maxprocs=" . $self->maxprocs . " timelimit=".$self->{timelimit});

    my $pool = AnyEvent::Fork
        ->new
        ->require ("CIHM::WIP::Ingest::Worker")
        ->AnyEvent::Fork::Pool::run 
        (
         "CIHM::WIP::Ingest::Worker::ingest",
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
    my $res = $self->wipmeta->get("/".$self->wipmeta->{database}."/_design/tdr/_view/ingestq?reduce=false&limit=".$self->limit.$skipparam,{}, {deserializer => 'application/json'});
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
        warn "_view/ingestq GET return code: ".$res->code."\n"; 
    }
    return;
}

1;
