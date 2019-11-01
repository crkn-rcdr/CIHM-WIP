package CIHM::WIP::unitize;

use strict;
use Carp;
use CIHM::WIP;
use Try::Tiny;
use JSON;
use Log::Log4perl;
use Net::Domain qw(hostname hostfqdn hostdomain domainname);
use File::Path qw(make_path);
use File::Spec;
use Data::Dumper;
use Email::MIME;
use Email::Sender::Simple qw(sendmail);

=head1 NAME

CIHM::WIP::unitize - Rearrange files within filesystem to match "OAIS packaging Object directory" structure.


=head1 SYNOPSIS

    my $wipmv = CIHM::WIP::unitize->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is a configuration file as defined in Config::General

=cut

BEGIN {
  Log::Log4perl->init_once("/etc/canadiana/wip/log4perl.conf");
}

sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    $self->{log} = Log::Log4perl->get_logger("CIHM::WIP::unitize");

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::TDR::Replication->new() not a hash\n";
    };
    $self->{args} = $args;

    $self->{WIP} = CIHM::WIP->new($self->configpath);
    $self->{hostname} = hostfqdn();

    # Set up array to accept warnings, to be sent in report.
    $self->{warnings}=[];

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

sub warnings {
    my $warning = shift;
    our $self;

    # Strip wide characters before  trying to log
    $warning =~ s/[^\x00-\x7f]//g;
    $self->log->warn($warning);

    push @{$self->{warnings}},$warning;

}



sub run {
    our ($self) = @_;

    $self->log->info("conf=".$self->configpath);

    # Capture warnings
    local $SIG{__WARN__} = sub { &warnings };

    my $config=$self->WIP->wipconfig->{unitize};

    if(defined $config->{lac_reel_ocr}) {
        my $base=$config->{lac_reel_ocr}{base};
        if (-d $base) {

            # Set up array to accept warnings and list of AIPs,
            # to be sent in report.

            $self->{aiplist}=[];
            $self->{warnings}=[];
            $self->{dirmove}=[];

            delete $self->{error};
            $self->scan_objid_dir({
                base => $base,
                trash => $config->{lac_reel_ocr}{trash},
                reject => $config->{lac_reel_ocr}{reject},
                objid_pattern => '^oocihm\.(.*)-sip$',
                glob => 'data/files/*',
                stage => 'Assembly',
                configid => 'heritage_ocr'
                                  });
            $self->gen_report({
                email => $config->{email},
                subject => 'Canadiana OAIS packaging Unitize report for: lac_reel_ocr'
                              });
        } else {
            $self->log->warn("$base not directory, but indicated as 'lac_reel_ocr'");
        }
    }

    if(defined $config->{lac_reel_ftp}) {
        my $base=$config->{lac_reel_ftp}{base};
        if (-d $base) {

            # Set up array to accept warnings and list of AIPs,
            # to be sent in report.

            $self->{aiplist}=[];
            $self->{warnings}=[];
            $self->{dirmove}=[];

            delete $self->{error};
            $self->scan_objid_dir({
                base => $base,
                trash => $config->{lac_reel_ftp}{trash},
                reject => $config->{lac_reel_ftp}{reject},
                objid_pattern => '^([CTH]-(.*))$',
                glob => '*.tif RAW/*.tif',
                stage => 'Assembly',
                configid => 'heritage'
                                  });
            $self->gen_report({
                email => $config->{email},
                subject => 'Canadiana OAIS packaging Unitize report for: lac_reel_ftp'
                              });
        } else {
            $self->log->warn("$base not directory, but indicated as 'lac_reel_ftp'");
        }
    }

    if(defined $config->{oop_ocr}) {
        my $base=$config->{oop_ocr}{base};
        if (-d $base) {

            # Set up array to accept warnings and list of AIPs,
            # to be sent in report.

            $self->{aiplist}=[];
            $self->{warnings}=[];
            $self->{dirmove}=[];

            delete $self->{error};
            $self->scan_objid_dir({
                base => $base,
                trash => $config->{oop_ocr}{trash},
                reject => $config->{oop_ocr}{reject},
                objid_pattern => '^oop\.(.*)-sip$',
                glob => 'data/files/*.jpg data/files/*.tif data/files/*.jp2',
                stage => 'OCR',
                configid => 'oop_issues'
                                  });
            $self->gen_report({
                email => $config->{email},
                subject => 'Canadiana OAIS packaging Unitize report for: oop_ocr'
                              });
        } else {
            $self->log->warn("$base not directory, but indicated as 'oop_ocr'");
        }
    }


}

sub gen_report {
    my ($self,$params) = @_;

    my $report = '';

    #
    # Get and add warnings to report
    #
    if (scalar(@{$self->{warnings}})) {
        $report .= "The following warnings were raised:\n\n";

        $report .= " * ".join(" * ",@{$self->{warnings}});
    }

    if (scalar(@{$self->{dirmove}})) {
        $report .= "\n\n\nThe following directories processed:\n\n";

        $report .= " * ".join(" * ",@{$self->{dirmove}});
    }

    if (scalar(@{$self->{aiplist}})) {
        $report .= "\n\n\nDirectories for the following identifiers were moved:\n\n";

        $report .= join("\n",@{$self->{aiplist}});
    }

    if ($report ne '') {
        my $email = $params->{email};
        my $subject = $params->{subject};
        if (!$subject) {
            $subject='Canadiana OAIS packaging Unitize report';
        }
            # Email or output to screen the report
        if (index($email,"@") == -1) {
            print $report;
        } else {
            my $message = Email::MIME->create(
                header_str => [
                    From    => '"Canadiana OAIS packaging Unitize Reporter" <noreply@canadiana.ca>',
                    To      => $email,
                    Subject => $subject,
                ],
                attributes => {
                    encoding => 'quoted-printable',
                    charset  => 'UTF-8',
                },
                body_str => $report,
                );
            sendmail($message);
        }
    }
}


sub scan_objid_dir {
    my ($self,$params) = @_;

    my $config=$self->WIP->configdocs->{$params->{configid}};
    if (!$config) {
        warn "ConfigID=".$params->{configid}." not valid\n";
        return 1;
    }
    my $depositor=$config->{depositor};
    if (!($self->WIP->depositor_valid($depositor))) {
        warn "Depositor=$depositor not valid\n";
        return 1;
    }

    my $path=$params->{base};
    my $trashpath=$params->{trash};
    my $rejectpath=$params->{reject};
    if (defined $params->{subdir}) {
        $path .= "/" . $params->{subdir};
        $trashpath .= "/" . $params->{subdir};
        $rejectpath .= "/" . $params->{subdir};
    }
    my $pattern=$params->{objid_pattern};
    my $error;
    if (opendir (my $dh, $path)) {
        while(readdir $dh) {
            next if $_ eq "." || $_ eq "..";
            my $file=$_;
            my $fullpath = $path."/$_";
            next if (! -d $fullpath);
            if(/$pattern/) {
                my $identifier=$1;
                my $dest=$self->WIP->stages->{$params->{stage}}."/".$params->{configid}."/".$identifier;
                my $objid=$self->WIP->i2objid($identifier,$params->{configid});
                if (!$self->WIP->objid_valid($objid)) {
                    warn "$objid not valid OBJID found in $fullpath\n";
                    $error=1;
                    $self->rename_dir($params,'reject',$file);
                    next;
                }
                my $aipdoc=$self->WIP->wipmeta->get_aip("$depositor.$objid");
                if ($aipdoc && exists $aipdoc->{filesystem}{stage}) {
                    my $fs=$aipdoc->{filesystem};
                    warn "AIP $depositor.$objid already exists at ".$self->WIP->stages->{$fs->{stage}}."/".$fs->{configid}."/".$fs->{identifier}." - Ignoring $fullpath\n";
                    $error=1;
                    $self->rename_dir($params,'reject',$file);
                    next;
                }
                if (-e $dest) {
                    warn "$dest already exists\n";
                    $error=1;
                    $self->rename_dir($params,'reject',$file);
                    next;
                }

                chdir $fullpath;
                my @mvfiles=glob($params->{glob});

                if (scalar(@mvfiles)) {
                    try {
                        make_path($dest);
                        foreach my $mvfile (@mvfiles) {
                            my @fileparts=split(/\//,$mvfile);
                            my $fname =pop @fileparts;
                            if (!rename($mvfile, "$dest/$fname")) {
                                die "Could not rename $mvfile to $dest/$fname: $!\n";
                            }
                        }
                        push @{$self->{aiplist}},$identifier;
                        $self->rename_dir($params,'trash',$file);
                    } catch {
                        warn "Caught error: $_";
                        $error=1;
                        $self->rename_dir($params,'reject',$file);
                        next;
                    };
                } else {
                    warn "There were no matching files in $fullpath\n";
                    $error=1;
                    $self->rename_dir($params,'reject',$file);
                    next;
                }
            } else {
                # Copy of parameters, as used in recursion
                my %params=%{$params};
                if (defined $params{subdir}) {
                    $params{subdir}.="/$file";
                } else {
                    $params{subdir}=$file;
                }
                $error |= $self->scan_objid_dir(\%params);

                $self->rename_dir($params, $error?'reject':'trash',$file);
            }
        }
        closedir $dh;
    } else {
        die "Couldn't open $path\n";
    }
    return $error;
}

sub rename_dir {
    my ($self,$params,$dest,$name) = @_;


    # Do nothing unless at the root
    if ($params->{subdir}) {
        return;
    }
    try {
        my $destdir=$params->{$dest};
        my $sourcedir=$params->{base};

        my $sourcename="$sourcedir/$name";
        my $destname=$destdir."/".$name;

        # Ensure destination name is unique
        my $count=0;
        while (-e $destname) {
            $count++;
            $destname=$destdir."/".$name.".$count";
        }

        if (! -d $destdir) {
            make_path($destdir);
        }
        rename ($sourcename, $destname) || die "Could not rename $sourcename to $destname: $!\n";
        push @{$self->{dirmove}},"Renamed $sourcename to $destname\n";
    } catch {
        warn "Caught error: $_";
    };
}

1;
