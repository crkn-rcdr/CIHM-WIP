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
use CIHM::WIP::Export::ExtractDmd;
use DateTime::Format::ISO8601;
use Archive::BagIt::Fast;

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
    if (!$self->swift) {
        die "CIHM::TDR::Swift instance parameter is mandatory\n";
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
sub swift {
    my $self = shift;
    return $self->args->{swift};
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

    my $swiftbag;
    my $swiftfile;
    my $swiftdest;
    switch ($exportReq->{type}) {
       case "aip"  {
           $swiftbag=$self->aip;
           if ($isfs) {
               $swiftdest=$destdir."/aip";
           } else {
               $swiftdest=$destdir."/$aip-aip";
           }
       }
       case "sip"  {
           $swiftbag=$self->aip."/data/sip/";
           if ($isfs) {
               $swiftdest=$destdir."/sip";
           } else {
               $swiftdest=$destdir."/$aip-sip";
           }
       }
       case /^(METS|dmdSec)$/  {
           $swiftfile=$self->aip."/data/sip/data/metadata.xml";
           if ($isfs) {
               $swiftdest=$destdir."/".$exportReq->{type}.".xml";
           } else {
               $swiftdest=$destdir."/$aip-".$exportReq->{type}.".xml";
           }
       }
       else {
           die "Unknown export type:".$exportReq->{type}."\n";
       }
    }
    if (-e $swiftdest) {
	die $swiftdest." already exists.\n";
    }
    print Dumper ($swiftbag, $swiftfile, $swiftdest);
    if (defined $swiftbag) {
	# Copy an entire Bagit
	mkdir $swiftdest
	    || die "Could not mkdir $swiftdest: $!\n";

	# Try to copy 3 times before giving up.
	my $success=0;
	for (my $tries=3 ; ($tries > 0) && ! $success ; $tries --) {
	    try {
		$self->swift->bag_download($swiftbag,$swiftdest);
		$success=1;
	    };
	}
	die "Error downloading $swiftbag from Swift\n" if (! $success);
	my $verified;
	try {
	    my $bagit = new Archive::BagIt::Fast($swiftdest);
	    my $valid = $bagit->verify_bag();
	    $verified = $valid;
	};
	if (!$verified) {
	    # Bag wasn't valid.
	    die "Error verifying bag: $swiftdest\n";
	}
    } else {
	# Copy single file

	# Try to copy 3 times before giving up.
	my $success=0;
	for (my $tries=3 ; ($tries > 0) && ! $success ; $tries --) {
	    try {
		my $object = $self->swift->swift->object_get($self->swift->container,$swiftfile);
		if ($object->code != 200) {
		    warn "object_get container: '".$self->swift->container."' , object: '$swiftfile'  returned ". $object->code . " - " . $object->message. "\n";
		} else {
		    open(my $fh, '>:raw', $swiftdest)
			or die "Could not open file '$swiftdest' $!";
		    print $fh $object->content;
		    close $fh;
		    my $filemodified = $object->object_meta_header('File-Modified');
		    if ($filemodified) {
			my $dt = DateTime::Format::ISO8601->parse_datetime( $filemodified );
			if (! $dt) {
			    die "Couldn't parse ISO8601 date from $filemodified\n";
			}
			my $atime=time;
			utime $atime, $dt->epoch(), $swiftdest;
		    }
		    $success=1;
		};
	    } catch {
		$self->log->warn("Caught error while downloading $swiftfile from Swift: $_");
	    };
	}
	die "Error downloading $swiftfile from Swift\n" if (! $success);
    }
    $self->log->info("$aip: Completed Swift copy to $swiftdest");

    if ($exportReq->{type} eq 'dmdSec') {
	my $newdest=$swiftdest;
	$newdest =~ s/-dmdSec\.xml//;
	CIHM::WIP::Export::ExtractDmd::extract($swiftdest,$newdest);
	unlink $swiftdest or warn "Could not unlink $swiftdest: $!\n";
    }

    return $exportdoc;
}

1;
