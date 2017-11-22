package CIHM::WIP::move;

use strict;
use Carp;
use CIHM::WIP;
use Try::Tiny;
use JSON;
use Log::Log4perl;
use Net::Domain qw(hostname hostfqdn hostdomain domainname);
use File::Path qw(make_path);

=head1 NAME

CIHM::WIP::move - Move files within WIP filesystem based on processing requests.


=head1 SYNOPSIS

    my $wipmv = CIHM::WIP::move->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as defined in CIHM::TDR::TDRConfig

=cut

BEGIN {
  Log::Log4perl->init_once("/etc/canadiana/wip/log4perl.conf");
}

sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    $self->{log} = Log::Log4perl->get_logger("CIHM::WIP::move");

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::TDR::Replication->new() not a hash\n";
    };
    $self->{args} = $args;

    $self->{WIP} = CIHM::WIP->new($self->configpath);
    $self->{hostname} = hostfqdn();


    return $self;
}

# Simple accessors for now -- Do I want to Moo?
sub args {
    my $self = shift;
    return $self->{args};
}
sub log {
    my $self = shift;
    return $self->{log};
}
sub configpath {
    my $self = shift;
    return $self->{args}->{configpath};
}
sub WIP {
    my $self = shift;
    return $self->{WIP};
}


sub run {
    our ($self) = @_;

    $self->log->info("conf=".$self->configpath);

    # Capture warnings
    local $SIG{__WARN__} = sub { &warnings };

    while (my $request = $self->getNextAIP) {

        my $aip = $request->{id};
        my $doc = $request->{doc};
        my $date = $request->{key};


        my $processdoc = {};
        my $status;
        $self->{aip}=$aip;
        $self->{message}='';

        # Handle and record any errors
        try {
            $status = JSON::true;
            $self->move($aip,$doc);
        } catch {
            $status = JSON::false;
            $self->log->error("$aip: $_");
            $self->{message} .= "Caught: " . $_;
        };
        $processdoc->{status}=$status;
        $processdoc->{message}=$self->{message};
        $processdoc->{request}='move';
        $processdoc->{reqdate}=$date;
        $processdoc->{host}=$self->hostname;
        $self->postResults($aip,$processdoc);
    }
}

sub move {
    my ($self,$aip,$doc) = @_;

    my $configdocs=$self->WIP->configdocs ||
        die "Can't retrieve configuration documents\n";

    my $req=$doc->{processReq}[0];
    my $fs=$doc->{filesystem};


    my $sourcedir;
    if ($fs && $fs->{stage} && $fs->{configid} && $fs->{identifier} &&
        defined $self->WIP->stages->{$fs->{stage}}) {
        $sourcedir=$self->WIP->stages->{$fs->{stage}}."/".$fs->{configid}."/".$fs->{identifier};
    }

    my $stage=$req->{stage};
    die "Stage '$stage' not defined\n"
        if (! defined $self->WIP->stages->{$stage});

    my $configid;
    if (defined $req->{configid}) {
        $configid=$req->{configid};
    } else {
        $configid=$fs->{configid};
    }
    if (! defined $configdocs->{$configid}) {
        warn("Destination configid '$configid' invalid\n");
        return;
    }

    my $identifier;
    if (defined $req->{identifier}) {
        $identifier=$req->{identifier};
    } else {
        $identifier=$fs->{identifier};
    }
    die "Destination identifier not defined\n"
        if (!$identifier);

    my $destparent = $self->WIP->stages->{$stage}."/".$configid; 
    my $destdir = "$destparent/$identifier";


    if ($sourcedir eq $destdir) {
        $self->log->info("No move required: $destdir");
    } elsif (-e $destdir) {
        die "Destination $destdir already exists.\n";
    } else {
        if (! -d $destparent) {
            make_path($destparent) || die "$aip($identifier): Could not make path $destparent: $!\n";
        }
        if (!$sourcedir || ! -d $sourcedir) {
            if (defined $req->{empty} && $req->{empty}) {
                mkdir $destdir 
                    or die "Could not create $destdir: $!\n";
                $self->log->info("created empty directory $destdir");
            } else {
                if ($sourcedir) {
                    die "$sourcedir not found or not directory\n";
                } else {
                    warn ("Can't move as source directory not defined in database\n");
                    return;
                }
            }
        } else {
            rename $sourcedir,$destdir 
                or die "Could not rename $sourcedir to $destdir: $!\n";
            $self->log->info("renamed $sourcedir to $destdir");
        }
        # Notify DB
        my $retdata = $self->WIP->wipmeta->update_filesystem(
            $aip,
            {
                "filesystem" => encode_json({
                    stage => $stage,
                    configid => $configid,
                    identifier => $identifier
                                            })
            });
    }
}


sub postResults {
    my ($self,$aip,$processdoc) = @_;

    my $ret = $self->WIP->wipmeta->update_basic($aip,{ 
        "processed" => encode_json($processdoc)
                                      });
    if ($ret ne 'update') {
        $self->log->error("postResults returned: $ret");
    }
}

sub warnings {
    my $warning = shift;
    our $self;
    my $aip="unknown";

    if ($self) {
        $self->{message} .= $warning;
        $aip = $self->{aip};
    }
    # Strip wide characters before  trying to log
    $warning =~ s/[^\x00-\x7f]//g;
    $self->log->warn($aip.": $warning");
}

sub getNextAIP {
    my $self = shift;

    $self->WIP->wipmeta->type("application/json");
    my $res = $self->WIP->wipmeta->get("/".$self->WIP->wipmeta->database."/_design/tdr/_view/wipmvq?reduce=false&limit=1&include_docs=true",{}, {deserializer => 'application/json'});
    if ($res->code == 200) {
        if (exists $res->data->{rows}) {
            return (@{$res->data->{rows}})[0];
        }
    }
    else {
        $self->log->error("_view/wipmvq GET return code: ".$res->code); 
    }
    return;
}


1;
