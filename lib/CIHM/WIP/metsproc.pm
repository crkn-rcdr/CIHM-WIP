package CIHM::WIP::metsproc;

use strict;
use Carp;
use CIHM::WIP;
use Try::Tiny;
use Capture::Tiny ':all';
use Switch;
use JSON;
use Log::Log4perl;
use Net::Domain qw(hostname hostfqdn hostdomain domainname);
use File::Path qw(make_path);
use File::Spec;
use Data::Dumper;
use Email::MIME;
use Email::Sender::Simple qw(sendmail);
use CIHM::METS::App;

=head1 NAME

CIHM::WIP::metsproc - Rearrange files within WIP filesystem to match "WIP Object directory" structure.


=head1 SYNOPSIS

    my $wipmv = CIHM::WIP::metsproc->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is a configuration file as defined in Config::General

=cut

BEGIN {
  Log::Log4perl->init_once("/etc/canadiana/wip/log4perl.conf");
}

sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    $self->{log} = Log::Log4perl->get_logger("CIHM::WIP::metsproc");

    if (ref($args) ne "HASH") {
        die "Argument to CIHM::TDR::Replication->new() not a hash\n";
    };
    $self->{args} = $args;

    $self->{WIP} = CIHM::WIP->new($self->configpath);
    $self->{hostname} = hostfqdn();

    # Set up array to accept warnings, to be sent in report.
    $self->{warnings}=[];

    # Set up array to accept output, to be sent in report.
    $self->{output}=[];

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

    push $self->{warnings},$warning;
}



sub run {
    our ($self) = @_;

    $self->log->info("conf=".$self->configpath);

    # Capture warnings
    local $SIG{__WARN__} = sub { &warnings };

    my $config=$self->WIP->wipconfig->{metsproc};

    if(defined $config->{base}) {
        my $base=$config->{base};
        if (-d $base) {
            my $configdocs=$self->WIP->configdocs();
            if (!$configdocs) {
                die "Can't acquire configuration docs\n";
            }

            # Set up array to accept warnings and list of AIPs,
            # to be sent in report.

            $self->{aiplist}=[];
            $self->{warnings}=[];
            $self->{dirmove}=[];
            delete $self->{error};

            foreach my $configid (keys $configdocs) {
                my $configdir=File::Spec->catdir($base,$configid);
                if (! -d $configdir) {
                    make_path($configdir) 
                        || die "Could not make configdir $configdir: $!\n";
                }

                my $trash=File::Spec->catdir($config->{trash},$configid);
                if(! -d $trash) {
                    make_path($trash) 
                        || die "Could not make trash dir $configdir: $!\n";
                }

                my $reject=File::Spec->catdir($config->{reject},$configid);
                if (! -d $reject) {
                    make_path($reject) 
                        || die "Could not make reject dir $configdir: $!\n";
                }

                $self->scan_config_dir({
                    base => $configdir,
                    trash => $trash,
                    reject => $reject,
                    configid => $configid,
                                  });
            }

            $self->gen_report({
#                email => "out",
                email => $config->{email},
                subject => 'Canadiana Metadata Process report'
                              });

        } else {
            $self->log->warn("$base not directory");
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

    if (scalar(@{$self->{output}}))  {
        $report .= "The following output was captured:\n\n";

        $report .= join("\n",@{$self->{output}});
    }

    if ($report ne '') {
        my $email = $params->{email};
        my $subject = $params->{subject};
        if (!$subject) {
            $subject='Canadiana METSproc report';
        }
            # Email or output to screen the report
        if (index($email,"@") == -1) {
            print STDERR $report;
        } else {
            my $message = Email::MIME->create(
                header_str => [
                    From    => '"Canadiana METSproc Reporter" <noreply@canadiana.ca>',
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

sub find_my_metsproc {
    my ($pathname,$metsproc) = @_;

    if (ref($metsproc) eq "HASH") {
        if (!$metsproc->{regex} ||
            $pathname =~ $metsproc->{regex}) {
            return $metsproc;
        }
    } elsif (ref($metsproc) eq "ARRAY") {
        foreach my $thismetsproc (@{$metsproc}) {
            my $ametsproc=find_my_metsproc($pathname,$thismetsproc);
            if ($ametsproc->{command}) {
                return $ametsproc;
            }
        }
    }
    # If nothing matches, then return blank
    return {};
}


sub scan_config_dir {
    my ($self,$params) = @_;
# base, trash, reject, configid

    my $configid=$params->{configid};
    my $config=$self->WIP->configdocs->{$configid};
    if (!$config) {
        warn "ConfigID=$configid not valid\n";
        return 1;
    }
    my $depositor=$config->{depositor};
    if (!$depositor) {
        $depositor="-unknown-";
    }
    if (!($self->WIP->depositor_valid($depositor))) {
        warn "Depositor=$depositor not valid for configid=$configid\n";
        return 1;
    }

    my $path=$params->{base};
    my $trashpath=$params->{trash};
    my $rejectpath=$params->{reject};

    my $error;
    my @paths;
    if (opendir (my $dh, $path)) {
        while(readdir $dh) {
            next if $_ eq "." || $_ eq "..";
            my $fullpath = $path."/$_";
            if (-d $fullpath) {
                warn "skipping invalid directory $fullpath\n";
                next;
            }
            push @paths,$fullpath;            
        }
        closedir $dh;
        if (@paths) {
            foreach my $pathname (@paths) {
                $self->log->info("Found: $pathname");

                my $thisoutput = capture_merged {
                    my $error=0;
                    if(defined $config->{metsproc}) {
                        my $metsproc=find_my_metsproc($pathname,$config->{metsproc});
                        my $mycommand=$metsproc->{command};
                        if (!$mycommand) {
                            $mycommand="-undefined-";
                        }
                        my @command;
                        switch ($mycommand) {
                            case "ignore" {}
                            case /^(csv2issue|csv2dc|dbtext2lac|dbtext2news|marc)$/ {
                                @command=("mets", # TODO: in config file?
                                          $mycommand);
                                if ($metsproc->{extraparam}) {
                                    if (ref $metsproc->{extraparam} eq 'ARRAY') {
                                        push @command, @{$metsproc->{extraparam}};
                                    } else {
                                        push @command, $metsproc->{extraparam};
                                    }
                                }
                                push @command, ($configid,$pathname);
                            }
                            else {
                                print STDERR "Command=$mycommand is not understood. configid=$configid, pathname=$pathname\n";
                                $error=1;
                            }
                        }
                        print STDERR "Running: @command\n";
                        if (scalar(@command)) {
                            my $ret = system(@command);
                            if ($ret != 0) {
                                warn "ERROR: shell command @command failed: $?\n";
                                print STDERR "Command failed\n";
                                $error=1;
                            }
                        }
                    } else {
                        print STDERR "No Metadata Processing type defined for $configid\n";
                        $error=1;
                    }
                    $self->rename_mdfile($pathname,($error?$rejectpath:$trashpath));
                };
                push $self->{output},$thisoutput;
            };
        }
    } else {
        die "Couldn't open $path\n";
    }
    return $error;
}

sub rename_mdfile {
    my ($self,$source,$dest) = @_;

    my ($vol,$dirs,$filename) =
        File::Spec->splitpath($source);

    my $destname=File::Spec->catfile($dest,$filename);

    # Ensure destination name is unique
    my $count=0;
    while (-e $destname) {
        $count++;
        $destname=File::Spec->catfile($dest,$filename.".".$count);
    }
    my $destdir=File::Spec->updir($destname);
    if (! -d $destdir) {
        make_path($destdir) || die "Could not make path $destdir: $!\n";
    }
    rename $source, $destname || die "Could not rename $source to $destname: $!\n";
    print STDERR "Renamed $source to $destname\n";
    $self->log->info("Renamed $source to $destname");
}

1;
