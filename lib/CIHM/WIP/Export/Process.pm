package CIHM::WIP::Export::Process;

use 5.014;
use strict;
use Try::Tiny;
use File::Spec;
use File::Path qw(make_path remove_tree);
use File::Copy;
use JSON;
use Switch;
use POSIX qw(strftime);


use Data::Dumper;

=head1 NAME

CIHM::WIP::Export::Process - Handles the processing of individual AIPs for CIHM::WIP::Export

=head1 SYNOPSIS

    my $t_repo = CIHM::WIP::Export::Process->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as defined in CIHM::TDR::TDRConfig

=cut


sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::TDR::Replication->new() not a hash\n";
    };
    $self->{args} = $args;

    if (!$self->aip) {
        die "Parameter 'aip' is mandatory\n";
    }
    if (!$self->WIP) {
        die "CIHM::WIP instance parameter is mandatory\n";
    }
    if (!$self->cserver) {
        die "cserver object parameter is mandatory\n";
    }
    if (!$self->log) {
        die "log object parameter is mandatory\n";
    }
    $self->{aipdata}=$self->wipmeta->get_aip($self->aip);
    if (!$self->aipdata) {
        die "Failed retrieving AIP data\n";
    }
    return $self;
}

# Simple accessors for now -- Do I want to Moo?
sub args {
    my $self = shift;
    return $self->{args};
}
sub aip {
    my $self = shift;
    return $self->args->{aip};
}
sub aipdata {
    my $self = shift;
    return $self->{aipdata};
}
sub log {
    my $self = shift;
    return $self->args->{log};
}
sub WIP {
    my $self = shift;
    return $self->args->{WIP};
}
sub wipmeta {
    my $self = shift;
    return $self->WIP->{wipmeta};
}
sub cserver {
    my $self = shift;
    return $self->args->{cserver};
}
sub configdocs {
    my $self = shift;
    return $self->WIP->configdocs;
}


sub export {
    my ($self) = @_;

    my $aip=$self->aip;
    my $exportdoc={};
    my $exportReq=$self->aipdata->{'processReq'}[0];

    $self->log->info("$aip: Accepted job. exportReq = ". encode_json($exportReq));
    $exportdoc->{'exportReq'}=$exportReq;

    my $aipinfo;
    # Find first configured repository which has the relevant AIP.
    # Doesn't check if latest version, and assumes user won't try to 
    # export immediately after import.
    my @rrepos = $self->cserver->replication_repositories();
    foreach my $repo (@rrepos) {
        $aipinfo=$self->cserver->get_aipinfo($aip,$repo);
        if (exists $aipinfo->{rsyncpath}) {
            last;
        } else {
            undef $aipinfo;
        }
    }
    if (!$aipinfo) {
        die "Couldn't get rsync path information for $aip\n";
    }

    my $isfs=exists $exportReq->{fs}; 
    my $destdir;
    if ($isfs) {
        my $stage=$exportReq->{fs}->{stage};
        my $configid=$exportReq->{fs}->{configid};
        my $identifier=$exportReq->{fs}->{identifier};

        # Current location always overrides request suggestion
        if (exists $self->aipdata->{filesystem} &&
            exists $self->aipdata->{filesystem}->{stage} &&
            exists $self->aipdata->{filesystem}->{stage} ne '') {
            $stage=$self->aipdata->{filesystem}->{stage};
            $configid=$self->aipdata->{filesystem}->{configid};
            $identifier=$self->aipdata->{filesystem}->{configid};
        }
        my $checkstage=$self->WIP->findstagei($stage);
        if (!$checkstage) {
            die "stage=$stage not valid\n";
        } else {
            $stage=$checkstage;
        }
        if (!($self->WIP->configid_valid($configid))) {
            die "configid=$configid not valid\n";
        }
        my $objid=$self->WIP->i2objid($identifier,$configid);
        if (!($self->WIP->objid_valid($objid))) {
            die "identifier=$identifier -> objid=$objid not valid\n";
        }
        my ($testd,$testi)=split(/\./,$aip);
        if ($objid ne $testi) {
            die "identifier=$identifier -> objid=$objid doesn't match $aip\n";
        }
        
        $destdir=$self->WIP->stages->{$stage}."/$configid/$identifier";
        if (! -d $destdir) {
            make_path($destdir) || die "Could not make path $destdir: $!\n";
            $self->log->info("Created $destdir")
        }
    } elsif (exists $exportReq->{wipDir}) {
        my $wipdir=$self->WIP->wipconfig->{wipdir};
        die "wipdir= not set in config\n" if (!$wipdir);
        die "wipdir=$wipdir not directory\n" if (! -e $wipdir);

        $destdir=$wipdir."/".$exportReq->{wipDir};
        if (! -d $destdir) {
            if ( $exportReq->{createdest} ) {
                make_path($destdir) || die "Could not make path $destdir: $!\n";
                $self->log->info("Created $destdir")
            } else {
                die "Destination directory $destdir doesn't exist\n";
            }
        }
    }

    my $rsyncsource;
    my $rsyncdest;
    switch ($exportReq->{type}) {
       case "aip"  {
           $rsyncsource=$aipinfo->{rsyncpath}."/.";
           if ($isfs) {
               $rsyncdest=$destdir."/aip";
           } else {
               $rsyncdest=$destdir."/$aip-aip";
           }
           if (-e $rsyncdest) {
               die $rsyncdest." already exists.\n";
           } else {
               mkdir $rsyncdest 
                    || die "Could not mkdir $rsyncdest: $!\n";
               $rsyncdest .= "/.";
           }
       }
       case "sip"  {
           $rsyncsource=$aipinfo->{rsyncpath}."/data/sip/.";
           if ($isfs) {
               $rsyncdest=$destdir."/sip";
           } else {
               $rsyncdest=$destdir."/$aip-sip";
           }
           if (-e $rsyncdest) {
               die $rsyncdest." already exists.\n";
           } else {
               mkdir $rsyncdest 
                    || die "Could not mkdir $rsyncdest: $!\n";
               $rsyncdest .= "/.";
           }
       }
       case "metadata"  {
           $rsyncsource=$aipinfo->{rsyncpath}."/data/sip/data/metadata.xml";
           if ($isfs) {
               $rsyncdest=$destdir."/metadata.xml";
           } else {
               $rsyncdest=$destdir."/$aip-metadata.xml";
           }
           if (-e $rsyncdest) {
               die $rsyncdest." already exists.\n";
           }
       }
       else {
           die "Unknown export type:".$exportReq->{type}."\n";
       }
    }
    $self->rsync($rsyncsource,$rsyncdest);
    $self->log->info("$aip: Completed rsync(\"$rsyncsource\" , \"$rsyncdest\")");
    return $exportdoc;
}


sub rsync {
    my ($self,$source,$destination) = @_;

    # Don't preserve owner or group, so run as intended user.
    my @rsynccmd=("rsync","-rlpt","--del","--partial","--timeout=10",$source,$destination);

    my $rsyncexit = 30;
    # https://download.samba.org/pub/rsync/rsync.html
    # 10 - Error in socket I/O
    # 12 - Error in rsync protocol data stream
    # 30 - Timeout in data send/receive
    while ($rsyncexit == 10 || $rsyncexit == 12 ||  $rsyncexit == 30 ) {
        system(@rsynccmd);
        if ($? == -1) {
            die "@rsynccmd -- failed to execute: $!\n";
        }
        elsif ($? & 127) {
            die "@rsynccmd -- nchild died with signal %d, %s coredump\n",
            ($? & 127),  ($? & 128) ? 'with' : 'without';
        }
        $rsyncexit =  $? >> 8;
    }
    if ($rsyncexit) {
        die "@rsynccmd -- child exited with value $rsyncexit\n";
    }
}

1;
