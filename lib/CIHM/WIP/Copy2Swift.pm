package CIHM::WIP::Copy2Swift;

use strict;
use Carp;
use Config::General;
use CIHM::TDR::Repository;
use CIHM::TDR::REST::wipmeta;
use CIHM::WIP::Copy2Swift::Worker;

use Coro::Semaphore;
use AnyEvent::Fork;
use AnyEvent::Fork::Pool;

use Try::Tiny;
use JSON;
use Data::Dumper;

=head1 NAME

CIHM::WIP::Copy2Swift - Copy modified AIPs to Swift


=head1 SYNOPSIS

    my $hammer = CIHM::WIP::Copy2Swift->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as defined in Config::General

=cut

sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::WIP::Copy2Swift->new() not a hash\n";
    };
    $self->{args} = $args;

    Log::Log4perl->init_once("/etc/canadiana/tdr/log4perl.conf");
    $self->{logger} = Log::Log4perl::get_logger("CIHM::TDR");

    my %confighash = new Config::General(
        -ConfigFile => $args->{configpath},
        )->getall;

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

    # Undefined if no <copy2swift> config block
    if (exists $confighash{copy2swift}) {
        if (exists $confighash{copy2swift}{aipdir}) {
            my $aipdir = $confighash{copy2swift}{aipdir};
            if (! -d $aipdir || ! -w $aipdir) {
                die "Path $aipdir is not a directory or not writeable\n";
            }
        } else {
            die "Missing aipdir option in <copy2swift> configuration block\n";
        }
    } else {
        die "Missing <copy2swift> configuration block\n";
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
    # Undefined if no <tdrepo> config block
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

sub copy {
    my ($self) = @_;


    $self->log->info("copy2swift: conf=".$self->configpath." skip=".$self->skip. " limit=".$self->limit. " maxprocs=" . $self->maxprocs . " timelimit=".$self->{timelimit});

    my $pool = AnyEvent::Fork
        ->new
        ->require ("CIHM::WIP::Copy2Swift::Worker")
        ->AnyEvent::Fork::Pool::run 
        (
         "CIHM::WIP::Copy2Swift::Worker::copy",
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
    my $res = $self->wipmeta->get("/".$self->wipmeta->{database}."/_design/tdr/_view/copyingest2swift?reduce=false&limit=".$self->limit.$skipparam,{}, {deserializer => 'application/json'});
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
        warn "_view/copyingest2swift GET return code: ".$res->code."\n"; 
    }
    return;
}

1;
