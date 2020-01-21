package CIHM::WIP::Copy2Swift::Process;

use 5.014;
use strict;
use Try::Tiny;
use File::Spec;
use File::Path qw(make_path remove_tree);
use File::Copy;
use JSON;
use Archive::BagIt::Fast;
use Archive::BagIt;
use Switch;
use POSIX qw(strftime);
use Data::Dumper;

=head1 NAME

CIHM::WIP::Copy2Swift::Process - Handles Copying of AIPs to Swift after ingest

=head1 SYNOPSIS

    my $t_repo = CIHM::TDR::Copy2Swift::Process->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as defined in Config::General

=cut


sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::TDR::Replication->new() not a hash\n";
    };
    $self->{args} = $args;

    if (!$self->log) {
        die "Log::Log4perl::get_logger object parameter is mandatory\n";
    }
    if (!$self->tdr) {
        die "CIHM::TDR instance parameter is mandatory\n";
    }
    if (!$self->wipmeta) {
        die "wipmeta object parameter is mandatory\n";
    }
    if (!$self->swift) {
        die "swift object parameter is mandatory\n";
    }
    if (!$self->repo) {
        die "repo object parameter is mandatory\n";
    }
    if (!$self->aip) {
        die "Parameter 'aip' is mandatory\n";
    }
    if (!$self->aipdir) {
        die "Parameter 'aipdir' is mandatory\n";
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
sub copyReq {
    my $self = shift;
    return $self->aipdata->{'processReq'}[0];
}
sub aiprepos {
    my $self = shift;
    return $self->{aiprepos};
}
sub configpath {
    my $self = shift;
    return $self->args->{configpath};
}
sub log {
    my $self = shift;
    return $self->args->{log};
}
sub wipmeta {
    my $self = shift;
    return $self->args->{wipmeta};
}
sub tdr {
    my $self = shift;
    return $self->args->{tdr};
}
sub swift {
    my $self = shift;
    return $self->args->{swift};
}
sub repo {
    my $self = shift;
    return $self->args->{repo};
}
sub tdrepo {
    my $self = shift;
    return $self->swift->tdrepo;
}
sub aipdir {
    my $self = shift;
    return $self->args->{aipdir};
}

sub process {
    my ($self) = @_;

    my $aipdir = File::Spec->catdir($self->aipdir,$self->aip);

    $self->log->info($self->aip.": Verifying $aipdir bag");

    my $verified;
    try {
        my $bagit = new Archive::BagIt::Fast($aipdir);
        my $valid = $bagit->verify_bag();
        $verified = $valid;
    };
    if (!$verified) {
        # Bag wasn't valid.
        die "Error verifying AIP at $aipdir\n";
    }

    $self->log->info($self->aip.": Copying $aipdir to Swift");

    # Get basic information about AIP
    my $aipdoc = $self->repo->get_manifestinfo($aipdir);

    # Try to copy 5 times before giving up.
    my $success;
    for (my $tries=5 ; ($tries > 0) && ! $success ; $tries --) {
	    try {
	        $self->swift->bag_upload($aipdir,$self->aip);
	        $success=1;
	    };
	    if ($success) {
	        $self->log->info($self->aip.": Swift copy of $aipdir complete, Validating");
	        my $validate = $self->swift->validateaip($self->aip);
	        if ($validate->{'validate'}) {
		        $self->tdrepo->update_item_repository($self->aip, {
		            'manifest date' => $validate->{'manifest date'},
			        'manifest md5' => $validate->{'manifest md5'}
						      });
	        } else {
		        warn("validation of ".$self->aip." failed\n");
		    $success=0;
	    }
	};
	if (($tries > 0) && ! $success) {
	    sleep(30); # Sleep for 30 seconds before trying again
	}
    }
    die "Failure while uploading ".$self->aip." to Swift\n" if (!$success);

    # Remove temporary AIP build directory
    remove_tree($aipdir) or die("Failed to remove $aipdir: $!");

    $self->log->info($self->aip.": Done processing");

    return $aipdoc;
}

1;
