package CIHM::WIP::Ingest::Process;

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

CIHM::WIP::Ingest::Process - Handles the processing of individual AIPs for CIHM::WIP::Ingest

=head1 SYNOPSIS

    my $t_repo = CIHM::TDR::Ingest::Process->new($args);
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
    if (!$self->tempdir) {
        die "Parameter 'tempdir' is mandatory\n";
    }
    if (!$self->stages) {
        die "Parameter 'stages' is mandatory\n";
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
sub ingestReq {
    my $self = shift;
    return $self->aipdata->{'processReq'}[0];
}
sub ingesttype {
    my $self = shift;
    return $self->ingestReq->{'type'}
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
sub hostname {
    my $self = shift;
    return $self->args->{hostname};
}
sub tempdir {
    my $self = shift;
    return $self->args->{tempdir};
}
sub stages {
    my $self = shift;
    return $self->args->{stages};
}
sub aipdir {
    my $self = shift;
    return File::Spec->catfile($self->tempdir,$self->aip);
}

sub process {
    my ($self) = @_;

    $self->log->info($self->aip.": Accepted job. ingestReq = ". encode_json($self->ingestReq));

    my $aipdir = $self->aipdir;
    $self->ingest_setup($aipdir);


    switch ($self->ingesttype) {

        case "new"  {
            make_path("$aipdir/data/revisions") or die("Failed to create $aipdir/data/revisions: $!");
            $self->copy_sip($aipdir);
            $self->tdr->changelog($aipdir, "Created new AIP");
            $self->log->info($self->aip.": Created new AIP in $aipdir");
        }

        case "update" {
            my $revision_name = strftime("%Y%m%dT%H%M%S", gmtime(time));
            move("$aipdir/data/sip", "$aipdir/data/revisions/$revision_name") or
                die("Failed to move $aipdir/data/sip to $aipdir/data/revisions/$revision_name: $!");
            $self->copy_sip($aipdir);

            # Check for duplicate metadata.xml 
            $self->checkdup($aipdir);

            $self->tdr->changelog($aipdir, "Updated SIP; old SIP stored as revision $revision_name");
            $self->log->info($self->aip.": Updated SIP in $aipdir; old SIP stored as revision $revision_name");
        }

        case "metadata" {
            my $revision_name = strftime("%Y%m%dT%H%M%S.partial", gmtime(time));
            mkdir("$aipdir/data/revisions/$revision_name") or
                die("Failed to create $aipdir/revisions/$revision_name: $!");
            move("$aipdir/data/sip/data/metadata.xml", "$aipdir/data/revisions/$revision_name/metadata.xml") or
                die("Failed tp move $aipdir/data/sip/data/metadata.xml to $aipdir/data/revisions/$revision_name/metadata.xml: $!");
            copy("$aipdir/data/sip/manifest-md5.txt",  "$aipdir/data/revisions/$revision_name/manifest-md5.txt") or
                die("Failed to copy $aipdir/data/sip/manifest-md5.txt to $aipdir/data/revisions/$revision_name/manifest-md5.txt: $!");

            # Get metadata attachment
            my $res = $self->wipmeta->get("/".$self->wipmeta->database."/".$self->aip."/metadata.xml");
            if ($res->code == 200) {
                open(my $fh, '>', "$aipdir/data/sip/data/metadata.xml");
                # Store content without deserialization
                print $fh $res->response->content;
                close $fh;
            }
            else {
                die "Get of metadata.xml return code: ".$res->code."\n"; 
            }

            # Check for duplicate metadata.xml 
            $self->checkdup($aipdir);

            # Update the SIP bagit info
            Archive::BagIt->make_bag("$aipdir/data/sip");

            $self->tdr->changelog($aipdir, "Updated metadata record; old record stored in revision $revision_name");
            $self->log->info($self->aip.": Updated metadata record in $aipdir; old record stored in revision $revision_name");
        }
        else {
            die "ingest type of ".$self->ingestype." invalid\n";
        }
    }

    my $validatetemp=$self->tempdir."/sipvalidate";
    if (! -d $validatetemp) {
        make_path($validatetemp) or die("Failed to create $validatetemp: $!");
    }

    my $sip = CIHM::TDR::SIP->new("$aipdir/data/sip");
    $sip->validate(1,$validatetemp);

    $self->tdr->changelog($aipdir, $self->ingestReq->{'changelog'});
    $self->log->info($self->aip.": Changelog: ". $self->ingestReq->{'changelog'});

    # Generate BagIt information files for the AIP
    Archive::BagIt->make_bag($aipdir);

    # Get basic information about AIP
    my $ingestdoc = $self->repo->get_manifestinfo($aipdir);

    # Try to copy 3 times before giving up.
    my $success=0;
    for (my $tries=3 ; ($tries > 0) && ! $success ; $tries --) {
	try {
	    $self->swift->bag_upload($aipdir,$self->aip);
	    $success=1;
	};
    }
    die "Failure while uploading ".$self->aip." to $aipdir\n" if (!$success);

    $self->log->info($self->aip.": Swift copy of $aipdir");

    my $validate = $self->swift->validateaip($self->aip);

    if ($validate->{'validate'}) {
	$self->tdrepo->update_item_repository($self->aip, {
	    'manifest date' => $validate->{'manifest date'},
		'manifest md5' => $validate->{'manifest md5'}
					      });
    } else {
	die("validation of ".$self->aip." failed");
    }

    # Remove temporary AIP build directory
    remove_tree($aipdir) or die("Failed to remove $aipdir: $!");

    $self->log->info($self->aip.": Done processing");

    return $ingestdoc;
}


sub copy_sip {
    my ($self,$aipdir) = @_;

    make_path("$aipdir/data/sip") or die("Failed to create $aipdir/data/sip: $!");

    my $fromsip;
    if (exists $self->ingestReq->{'rsyncurl'}) {
        $fromsip=$self->ingestReq->{'rsyncurl'}."/.";
    } else {
        if (!defined $self->aipdata->{filesystem}) {
            die "Required filesystem field not defined\n";
        }
        my $stage = $self->aipdata->{filesystem}->{stage} or
            die "Filesystem sub-field 'stage' not defined\n";
        my $stagedir = $self->stages->{$stage} or
            die "WIP Filesystem stage=$stage not configured\n";

        my $configid = $self->aipdata->{filesystem}->{configid} or
            die "Filesystem sub-field 'configid' not defined\n";
        my $identifier = $self->aipdata->{filesystem}->{identifier} or
            die "Filesystem sub-field 'identifier' not defined\n";

        $fromsip=$stagedir."/".$configid."/".$identifier.
            "/sip/.";
    }

    $self->rsync($fromsip,"$aipdir/data/sip/.");

    my $verified;
    try {
        my $bagit = new Archive::BagIt::Fast("$aipdir/data/sip");
        my $valid = $bagit->verify_bag();
        $verified = $valid;
    };
    if (!$verified) {
        # Bag wasn't valid.
        die "Error verifying sip copied from: $fromsip\n";
    }
}


sub ingest_setup {
    my ($self,$aipdir) = @_;

    my $res = $self->tdrepo->post("/".$self->tdrepo->database."/_design/tdr/_view/newestaip?group=true",{ keys => [ $self->aip ]}, {deserializer => 'application/json'});
    if ($res->code == 200) {
        if (defined $res->data->{rows} && scalar(@{$res->data->{rows}})) {
            $self->{aiprepos} = \@{$res->data->{rows}->[0]->{value}[1]};
        }
    } else {
        die "_view/newestaip during ingest_setup() for ".$self->aip." returned: " . $res->code."\n";
    }


    if ($self->aiprepos) {
        if ($self->ingesttype eq 'new' ) {
            die "Type is new, but ".$self->aip." already exists in tdrepo\n";
        }

	my %findswift = map { $_ => 1 } @{$self->aiprepos};
	if (! exists($findswift{$self->swift->repository})) {
            die $self->aip." not found in Swift repository=".$self->swift->repository."\n";
        }

	mkdir $aipdir;

	# Try to copy 3 times before giving up.
	my $success=0;
	for (my $tries=3 ; ($tries > 0) && ! $success ; $tries --) {
	    try {
		$self->swift->bag_download($self->aip,$aipdir);
		$success=1;
	    };
	}
	die "Error downloading from Swift\n" if (! $success);

	my $verified;
	try {
	    my $bagit = new Archive::BagIt::Fast($aipdir);
	    my $valid = $bagit->verify_bag();
	    $verified = $valid;
	};
	if (!$verified) {
	    # Bag wasn't valid.
	    die "Error verifying bag: $aipdir\n";
	}
    } else {
        if ($self->ingesttype ne 'new' ) {
            die "Type is ".$self->ingesttype.", but ".$self->aip." doesn't exists in tdrepo\n";
        }
        if (-d $aipdir) {
            remove_tree($aipdir) or die("Failed to remove old AIP attempt at $aipdir: $!");
        } 
    }
}

sub checkdup {
    my ($self,$aipdir) = @_;

    my $aip = $self->aip;

    my $metadata="$aipdir/data/sip/data/metadata.xml";
    open(METADATA, "<", $metadata) or die "Can't open $metadata\n";
    binmode(METADATA);
    my $metadatamd5=Digest::MD5->new->addfile(*METADATA)->hexdigest;
    close(METADATA);

    # Check for duplicate metadata MD5
    open my $fh,"<$aipdir/manifest-md5.txt"
        or die("Can't open manifest file within $aipdir\n");
    while (my $line = <$fh>) {
        chomp($line);
        if (substr($line,-12) eq 'metadata.xml') {
            my ($md5,$filename)=split /\s+/, $line;
            if($md5 eq $metadatamd5) {
                die("metadata.xml from SIP matches existing AIP: $filename\n");
            }
        }
    }
    close ($fh);
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
