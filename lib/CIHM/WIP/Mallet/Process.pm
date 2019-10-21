package CIHM::WIP::Mallet::Process;

use 5.014;
use strict;
use Try::Tiny;
use File::Spec;
use File::Path qw(make_path remove_tree);
use File::Copy;
use File::Slurp;
use JSON;
use Switch;
use POSIX qw(strftime);
use Data::Dumper;
use XML::LibXML;
use BSD::Resource;
use Archive::BagIt;
use CIHM::TDR::SIP;

=head1 NAME

CIHM::WIP::Mallet::Process - Handles the processing of individual AIPs for CIHM::WIP::Mallet

=head1 SYNOPSIS

    my $t_repo = CIHM::WIP::Mallet::Process->new($args);
      where $args is a hash of arguments.

      $args->{configpath} is as defined in CIHM::WIP

=cut


sub new {
    my($class, $args) = @_;
    my $self = bless {}, $class;

    # pdfunite/pdftk needs 1 file open per PDF file it is joining
    setrlimit("RLIMIT_OPEN_MAX",2048,2048) or die "Can't set RLIMIT_OPEN_MAX\n";

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
    if (!$self->log) {
        die "log object parameter is mandatory\n";
    }
    if (!$self->hostname) {
        die "hostname parameter is mandatory\n";
    }

    $self->{aipdata}=$self->wipmeta->get_aip($self->aip);
    if (!$self->aipdata) {
        die "Failed retrieving AIP data\n";
    }

    my ($depositor,$objid) = split(/\./,$self->aip);
    $self->{depositor}=$depositor;
    $self->{objid}=$objid;

    # Where the current count for an get_id() for each type is stored
    $self->{id}={};

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
sub depositor {
    my $self = shift;
    return $self->{depositor};
}
sub objid {
    my $self = shift;
    return $self->{objid};
}
sub aipdata {
    my $self = shift;
    return $self->{aipdata};
}
sub hostname {
    my $self = shift;
    return $self->args->{hostname};
}
sub log {
    my $self = shift;
    return $self->args->{log};
}
sub WIP {
    my $self = shift;
    return $self->args->{WIP};
}
sub swift {
    my $self = shift;
    return $self->args->{swift};
}
sub container {
    my $self = shift;
    return $self->args->{swiftcontainer};
}
sub wipmeta {
    my $self = shift;
    return $self->WIP->{wipmeta};
}
sub configdocs {
    my $self = shift;
    return $self->WIP->configdocs;
}
sub configid {
    my $self = shift;
    return $self->{configid};
}
sub myconfig {
    my $self = shift;
    return $self->{myconfig};
}
sub workdir {
    my $self = shift;
    return $self->{workdir};
}
sub workdata {
    my $self = shift;
    return $self->workdir."/sip/data";
}
sub datafiles {
    my $self = shift;
    return $self->workdata."/files";
}
sub datameta {
    my $self = shift;
    return $self->workdata."/metadata";
}
sub components {
    my $self = shift;
    return $self->{components};
}
sub componentorder {
    my $self = shift;
    return $self->{componentorder};
}
sub distpdf {
    my $self = shift;
    return $self->{distpdf};
}
sub doc {
    my $self = shift;
    return $self->{doc};
}
sub amdsec {
    my $self = shift;
    return $self->{amdsec};
}


sub process {
    my ($self) = @_;

    $self->{job} = $self->aipdata->{'processReq'}[0];

    $self->log->info($self->aip.": Accepted job. processReq = ". encode_json($self->{job}));

    # Set more per-AIP information
    if (!defined $self->aipdata->{filesystem}) {
        die "Required filesystem field not defined\n";
    }

    $self->{configid} = $self->aipdata->{filesystem}->{configid} or
        die "Filesystem sub-field 'configid' not defined\n";
    $self->{myconfig} =$self->WIP->configdocs->{$self->configid} or
        die $self->configid." is not a valid configuration id\n";

    my $request=$self->{job}->{request};
    if ($request eq 'buildsip') {
        $self->build_sip();
    } elsif ($request eq 'manipmd') {
        $self->manip_md();
    } else {
        die "Processing request type $request not valid\n";
    }

    $self->log->info($self->aip.": Completed job.");
    return {};
}


sub scan_workdir {
    my ($self,$components) = @_;

    # This function fills in the following 
    $self->{components}=defined $components ? $components : {};
    # $self->{componentorder} set at end...

    my ($stage,$stagedir,$identifier);

    $stage = $self->aipdata->{filesystem}->{stage} or
        die "Filesystem sub-field 'stage' not defined\n";
    $stagedir = $self->WIP->stages->{$stage} or
        die "Filesystem stage=$stage not configured\n";

    $identifier = $self->aipdata->{filesystem}->{identifier} or
        die "Filesystem sub-field 'identifier' not defined\n";

    $self->{workdir}=$stagedir."/".$self->configid."/".$identifier;

    if (! -d $self->workdir) {
        die "Working directory ".$self->workdir." doesn't exist\n";
    }
    chdir $self->{workdir};

    $self->log->info($self->aip.": Workdir=".$self->workdir);


    my $sipbuilddir = $self->workdir."/sip-build";
    if (-d $sipbuilddir) {
        remove_tree($sipbuilddir);
    }
    make_path($sipbuilddir);


    my %pdffiles;
    my %xmlfiles;
    if (opendir (my $dh, $self->workdir)) {
        while(readdir $dh) {
            next if -d $_;  # Ignore all directories
            my $fileconfig=$self->WIP->find_fileconfig($self->configid,$_);
            next if ($fileconfig && $fileconfig->{ignore});

            my ($basename,$extension);
            if (/^(.*)\.([^\.]+)$/) {
                $basename = $1;
                $extension = $2;
            }
            switch ($extension) {
                case /(jpg|jp2|tif)/ {
                    if (defined $self->{components}{$basename}) {
                        die "Duplicate basename: $_ , ".$self->{components}{$basename}{image}."\n";
                    }
                    $self->{components}{$basename}{image}=$_;
                    $self->{components}{$basename}{ext}=$extension;
                }
                case "pdf" {
                    $pdffiles{$basename}=$_;
                }
                case "xml" {
                    $xmlfiles{$basename}=$_;
                }
                case "txt" {
                    # ignoring all text files
                }
                else {
                    warn "Unknown file $_\n";
                }
            }
        }
    } else {
        die "Couldn't open ".$self->workdir."\n";
    }

    # The basenames of the images need to be named such that when
    # alphabetically sorted they are in correct sequence order.
    my @componentorder=sort(keys $self->{components});

    # If config field exists, enforce it now that @componentorder is
    # based on the images found.
    if (exists $self->myconfig->{images}) {
        if ($self->{myconfig}->{images} && !@componentorder) {
            die "Required item images missing\n";
        }
        if (!($self->myconfig->{images}) && @componentorder) {
            die "Item images exists, but config indicates they should not\n";
        }
    }


    # Match up the XML files
    # Design: Do we want to support an item-level XML?
    foreach my $xml (keys %xmlfiles) {
        if (defined $self->{components}{$xml}) {
            $self->{components}{$xml}{'xml'}=1;
            delete $xmlfiles{$xml};
        } else {
            die "Unmatched XML file $xml\n";
        }
    }

    # Add PDF files related to images to component hash, as well as 
    # determine if distribution PDF exists
    my $componentpdf;
    foreach my $pdf (keys %pdffiles) {
        if (defined $self->{components}{$pdf}) {
            $self->{components}{$pdf}{pdf}=1;
            delete $pdffiles{$pdf};
            $componentpdf=1;
        } else {
            if ($self->distpdf) {
                die "More than one PDF file not connected to images remaining\n";
            } else {
                $self->{distpdf}=$pdf.".pdf";
            }
        }
    }
    if ($componentpdf) {
        # If any component image has a related PDF, then they all should.
        foreach my $component (keys $self->{components}) {
            if (!defined $self->{components}{$component}{pdf}) {
                die "Component $component missing associated PDF file\n";
            }
        }
    }

    # Grab component labels, or set default
    if (defined $self->aipdata->{'_attachments'}->{'labels.json'}) {
        my $attach=$self->WIP->wipmeta->get_attachment($self->aip,'labels.json')
            or die "labels.json couldn't be fetched\n";

        my $labeltemp = decode_json $attach;
        if (ref($labeltemp) eq 'ARRAY') {
            my @componentlabels = @{$labeltemp};
            if (scalar(@componentlabels) != scalar(@componentorder)) {
                die "Count of labels in labels.json of ".scalar(@componentlabels)." doesn't match count of components ".scalar(@componentorder)."\n";
            }
            for (my $i = 0; $i < @componentorder; $i++) {
                $self->{components}{$componentorder[$i]}{label}=$componentlabels[$i];
            }
        } else {
            die "Labels.json attachment not an array: $attach\n";
        }
    } else {
        # Default is to use image sequence
        for (my $i = 1; $i <= @componentorder; $i++) {
            $self->{components}{$componentorder[$i - 1]}{label}="Image $i";
        }
    }        

    # If config field exists, enforce it
    if (exists $self->myconfig->{itempdf}) {
        if ($self->myconfig->{itempdf} && !$self->distpdf) {
            die "Required item distribution pdf missing\n";
        }
        if (!($self->myconfig->{itempdf}) && $self->distpdf) {
            die "Item distribution pdf exists, but config indicates it should not\n";
        }
    }


    # Set this value with latest value before continuing.
    $self->{componentorder}=\@componentorder;
}


sub build_sip {
    my ($self) = @_;


    # Work directory always scanned when SIP being built.
    $self->scan_workdir();

    my $sipdir = $self->workdir."/sip";
    if (-d $sipdir) {
        remove_tree($sipdir);
    }

    make_path($self->datafiles);
    make_path($self->datameta);

    my $sipbuilddir = $self->workdir."/sip-build";


    # Presume for now that mets components are being used to create METS
    # TODO: allow SIP to be generated from attached metadata.xml
    { 
        # Mandatory label for item
        my $itemlabel = $self->aipdata->{label} or
            die "Mandatory item label missing\n";

        if (! defined $self->aipdata->{'_attachments'}->{'dmd.xml'}) {
            die "Mandatory dmd.xml attachment missing";
        }

        my $dmdattach=$self->WIP->wipmeta->get_attachment($self->aip,'dmd.xml')
            or die "Mandatory dmd.xml couldn't be fetched\n";

        # Create the METS record
        $self->{doc} = XML::LibXML::Document->new('1.0', 'UTF-8');
        my $doc=$self->doc;
        my $mets = $doc->createElement('mets:mets');

        $mets->setAttribute('xmlns:mets', 'http://www.loc.gov/METS/');
        $mets->setAttribute('xmlns:xlink', 'http://www.w3.org/1999/xlink');
        $mets->setAttribute('OBJID', $self->objid);
        $doc->setDocumentElement($mets);

        $self->{amdsec}= $doc->createElement('mets:amdSec');
        my $filesec = $doc->createElement('mets:fileSec');

        # Array of filegroups, which match item is index 0, components index >0;
        my @filegrps;

        my $structmap = $doc->createElement('mets:structMap');
        $structmap->setAttribute('TYPE', 'physical');
        my $structroot = $doc->createElement('mets:div');

        # Set information for item
        {
            my $dmd_doc= XML::LibXML->load_xml(string => $dmdattach);
            my $dmdid=$self->get_id("dmd");
            my $mdtype = add_dmdsec($doc,$mets,$dmd_doc, $dmdid,0);
            $self->check_item_dmdsec($mdtype);

            $structroot->setAttribute('TYPE', $self->myconfig->{type});
            $structroot->setAttribute('LABEL', $itemlabel);
            $structroot->setAttribute('DMDID', $dmdid);

            $filegrps[0]=$doc->createElement('mets:fileGrp');

            if ( $self->distpdf) {
                my ($junk,$junk,$distpdffile)=File::Spec->splitpath($self->distpdf);
                my $destdistpdf=join('/',$self->datafiles,$distpdffile);
                my $retval = link $self->distpdf, $destdistpdf;
                if( $retval != 1 ){
                    die "Error creating link from $destdistpdf to ".$self->distpdf.": $!\n";
                }
                my $did=$self->get_id("distribution");
                $self->add_file($filegrps[0], "distribution", $did, 'application/pdf', 'URN', $distpdffile);
                add_fptr($doc,$structroot, $did);
            }
            $structmap->appendChild($structroot);
        }
        
        # Set component-level information
        for (my $i = 1; $i <= @{$self->componentorder}; $i++) {
            my $basename=@{$self->componentorder}[$i - 1];
            my $component=$self->{components}{$basename};

            my $div = $doc->createElement('mets:div');
            $div->setAttribute('TYPE', 'page');
            $div->setAttribute('LABEL', $component->{label});
            $structroot->appendChild($div);

            $filegrps[$i]=$doc->createElement('mets:fileGrp');

            # If there are image files, add to SIP and METS
            if ($component->{image}) {
                my ($junk,$junk,$imagename)=File::Spec->splitpath($component->{image});
                my $retval = link $component->{image}, $self->datafiles."/".$imagename;
                if( $retval != 1 ) {
                    die "Error creating link from ".$self->datafiles."/$imagename to ",$component->{image}.": $!\n";
                }

                my $mime;
                if ($component->{ext} eq "jpg"){
                    $mime = 'image/jpeg';
                }
                elsif ($component->{ext} eq 'jp2') {
                    $mime = 'image/jp2';
                }
                elsif ($component->{ext} eq 'tif') {
                    $mime = 'image/tiff';
                }
                else {
                    die ("unknown extension: ".$component->{ext}." for component image $i\n");
                }
                my $mid=$self->get_id("master");
                $self->add_file($filegrps[$i], "master", $mid, $mime, 'URN', $imagename);
                add_fptr($doc,$div, $mid);
            }
            # If there is a PDF derivative, add to SIP and METS
            if ($component->{'pdf'}) {
                if (exists $self->myconfig->{componentpdf} && !$self->myconfig->{componentpdf}) {
                    die "Component PDF not allowed, but $basename has pdf\n";
                }
                my $pdfname=$basename.".pdf";
                my $retval = link $pdfname, $self->datafiles."/".$pdfname;
                if( $retval != 1 ) {
                    die "Error creating link for ".$self->datafiles."/$pdfname: $!\n";
                }
                my $did=$self->get_id("derivative");
                $self->add_file($filegrps[$i], "derivative", $did, 'application/pdf', 'URN', $pdfname);
                add_fptr($doc,$div, $did);
            } elsif (exists $self->myconfig->{componentpdf} && $self->myconfig->{componentpdf}) {
                die "Component PDF mandatory and $basename missing PDF\n";
            }
            # If there is an XML (OCR) derivative, add to SIP and METS
            if ($component->{'xml'}) {
                if (exists $self->myconfig->{componentxml} && !$self->myconfig->{componentxml}) {
                    die "Component XML not allowed, but $basename has xml\n";
                }
                my $xmlname=$basename.".xml";
                my $retval = link $xmlname, $self->datafiles."/".$xmlname;
                if( $retval != 1 ) {
                    die "Error creating link for ".$self->datafiles."/$xmlname: $!\n";
                }
                my $did=$self->get_id("derivative");
                $self->add_file($filegrps[$i], "derivative", $did, 'application/xml', 'URN', $xmlname);
                add_fptr($doc,$div, $did);
            } elsif (exists $self->myconfig->{componentxml} && $self->myconfig->{componentxml}) {
                die "Component XML mandatory and $basename missing XML\n";
            }
        }

        # Put all the parts together
        $mets->appendChild($self->amdsec);
        $mets->appendChild($filesec);
        foreach my $filegrp (@filegrps) {
            $filesec->appendChild($filegrp);
        }
        $mets->appendChild($structmap);

        $doc->toFile("$sipdir/data/metadata.xml", 1) or 
            die("Can't write metadata.xml to $sipdir/data: $!");
    }

    # Write the Bagit structure and manifest...
    Archive::BagIt->make_bag($sipdir);

    if ($self->{job}->{validate}) {
        # Use the Trashcan as a temporary directory when validating SIP.
        my $tempdir =  $self->WIP->stages->{'Trashcan'};
        if ($tempdir) {
            $tempdir .= "/sipvalidate";
            if (! -d $tempdir) {
                make_path($tempdir) or die("Failed to create $tempdir: $!");
            }
        }
        my $sip = CIHM::TDR::SIP->new($sipdir);
        $sip->validate(1,$tempdir);
        $self->log->info($self->aip.": Successfully validated.");
    }
}

sub add_fptr {
    my($doc,$div, $fileid) = @_;
    my $fptr = $doc->createElement('mets:fptr');
    $fptr->setAttribute('FILEID', $fileid);
    $div->appendChild($fptr);
}

# Temporarily handle in this function
# Will be running JHOVE in separate microservice
sub generate_jhove {
    my ($self,$relfilepath,$relmdpath,$mimetype) = @_;

    my $filepath= File::Spec->rel2abs($relfilepath,$self->workdata);
    my $mdpath= File::Spec->rel2abs($relmdpath,$self->workdata);

    my $module;
    switch ($mimetype) {
        case 'image/jpeg' {$module='JPEG-hul';}
        case 'image/jp2' {$module='JPEG2000-hul';}
        case 'image/tiff' {$module='TIFF-hul';}
        case 'application/pdf' {$module='PDF-hul';}
        case 'application/xml' {$module='XML-hul';}
        else {die "unknown mime type=$mimetype for $relfilepath\n";}
    }
    my @command=("/opt/jhove/jhove", # TODO: in config file?
                 "-k","-m",$module,"-h","xml","-o",$mdpath,$filepath);

    system(@command) == 0 
            or die "shell command @command failed: $?";

    # Parse file to adjust uri, but also verifies basic format of file.
    my $jhove = eval { XML::LibXML->new->parse_file($mdpath) };
    if ($@) {
        die "parse_file($mdpath): $@\n";
    }

    # Document should have only one <jhove> child
    my @jchildren=$jhove->childNodes();
    if (scalar(@jchildren) != 1) {
        die "$mdpath has ".scalar(@jchildren)." children\n";
    }
    my $jn=$jchildren[0];
    if ($jn->nodeName ne 'jhove') {
        die "$mdpath has ".$jn->nodeName." named node\n";
    }
    my $repcount=0;
    foreach my $thisnode ($jn->nonBlankChildNodes()) {
        if ($thisnode->nodeName eq 'repInfo') {
            $thisnode->setAttribute('uri',$relfilepath);
            $repcount++;
        }
    }
    if ($repcount != 1) {
        die "$mdpath didn't have exactly 1 repInfo children\n";
    }
    $jhove->toFile($mdpath,1);
}


sub add_file {
    my($self,$filegrp, $fileuse, $id, $mimetype, $loctype, $href) = @_;
    my $file = $self->doc->createElement('mets:file');
    $file->setAttribute('USE', $fileuse);
    my $flocat = $self->doc->createElement('mets:FLocat');
    if ($mimetype ne 'text/html') {

        my $admid="jhove_$id";

        my $relfilepath=$href;
        if ($loctype eq 'URN') {
            $relfilepath="files/$href"; 
        }
        my ($junk,$junk,$filename)=File::Spec->splitpath($relfilepath);
        my $relmdpath="metadata/$filename.jhove.xml";

        $self->generate_jhove($relfilepath,$relmdpath,$mimetype);

        my $techmd=$self->doc->createElement('mets:techMD');
        $techmd->setAttribute('ID',$admid);

        my $mdref=$self->doc->createElement('mets:mdRef');

        $mdref->setAttribute('LOCTYPE', 'URL');
        $mdref->setAttribute('xlink:href', $relmdpath);
        $mdref->setAttribute('MIMETYPE', 'text/xml');
        $mdref->setAttribute('MDTYPE', 'OTHER');
        $mdref->setAttribute('OTHERMDTYPE', 'jhove');

        $techmd->appendChild($mdref);
        $self->amdsec->appendChild($techmd);

        $file->setAttribute('ADMID', $admid);
    }
    $file->setAttribute('ID', $id);
    $file->setAttribute('MIMETYPE', $mimetype);
    $flocat->setAttribute('LOCTYPE', $loctype);
    $flocat->setAttribute('xlink:href', $href);
    $file->appendChild($flocat);
    $filegrp->appendChild($file);
}

sub add_dmdsec {
    my($doc,$mets,$dmd_doc, $dmdid,$replace) = @_;
    
    my $dmdsec;
    my $mdwrap;
    my $xmldata;
    my $mdtype;

    my $dmd_root = $dmd_doc->documentElement();
    my $dmd_xpc = XML::LibXML::XPathContext->new($dmd_root);
    $dmd_xpc->registerNs('mets', 'http://www.loc.gov/METS/');
    $dmd_xpc->registerNs('xlink', 'http://www.w3.org/1999/xlink');
    $dmd_xpc->registerNs('marc', 'http://www.loc.gov/MARC21/slim');
    $dmd_xpc->registerNs('issue', 'http://canadiana.ca/schema/2012/xsd/issueinfo');
    $dmd_xpc->registerNs('txt', 'http://canadiana.ca/schema/2012/xsd/txtmap');
    $dmd_xpc->registerNs('dc', 'http://purl.org/dc/elements/1.1/');

    $dmdsec = $doc->createElement('mets:dmdSec');
    $mdwrap = $doc->createElement('mets:mdWrap');
    $xmldata = $doc->createElement('mets:xmlData');

    $dmdsec->setAttribute('ID', $dmdid);

    if ($dmd_xpc->findnodes('/marc:collection')) {
        $mdwrap->setAttribute('MIMETYPE', 'text/xml');
        $mdwrap->setAttribute('MDTYPE', 'MARC');
        $mdtype='MARC';
    }
    elsif ($dmd_xpc->findnodes('/simpledc')) {
        $mdwrap->setAttribute('MIMETYPE', 'text/xml');
        $mdwrap->setAttribute('MDTYPE', 'DC');
        $mdtype='DC';
    }
    elsif ($dmd_xpc->findnodes('/issue:issueinfo')) {
        $mdwrap->setAttribute('MIMETYPE', 'text/xml');
        $mdwrap->setAttribute('MDTYPE', 'OTHER');
        $mdwrap->setAttribute('OTHERMDTYPE', 'issueinfo');
        $mdtype='issueinfo';
    }
    elsif ($dmd_xpc->findnodes('/txt:txtmap')) {
        $mdwrap->setAttribute('MIMETYPE', 'text/xml');
        $mdwrap->setAttribute('MDTYPE', 'OTHER');
        $mdwrap->setAttribute('OTHERMDTYPE', 'txtmap');
        $mdtype='txtmap';
    }
    else {
        die("Can't determine record type:\n" . $dmd_doc->toString(1));
    }

    $doc->adoptNode($dmd_root);
    $xmldata->appendChild($dmd_root);
    $mdwrap->appendChild($xmldata);
    $dmdsec->appendChild($mdwrap);

    if ($replace) {
        my @olddmdsec = $dmd_xpc->findnodes("mets:mets/mets:dmdSec[\@ID=\"$dmdid\"]",$doc);
        if (! @olddmdsec) {
            die "Couldn't find dmdSec ID=$dmdid in METS, can't substitute new dmdSec\n";
        }
        if (scalar (@olddmdsec) >1) {
            die "More than one dmdSec ID=$dmdid found in METS, can't substitute new dmdSec\n";
        }
        $olddmdsec[0]->replaceNode($dmdsec);
    } else {
        $mets->appendChild($dmdsec);
    }

    return $mdtype;
}


sub manip_md {
    my ($self) = @_;

    if (!($self->swift)) {
        die "<swift> config missing from config file\n";
    }
    my $mdfile=$self->aip."/data/sip/data/metadata.xml";
    my $r = $self->swift->swift_object_get($self->container, $mdfile);
    if ($r->code != 200) {
        warn "Error: ".$r->error."\n" if $r->error;
        die "Swift get of $mdfile returned".$r->code."\n";
    }

    my $doc=XML::LibXML->new->parse_string($r->content);
    my $xpc = XML::LibXML::XPathContext->new($doc);
    $xpc->registerNs('mets', 'http://www.loc.gov/METS/');
    $xpc->registerNs('xlink', 'http://www.w3.org/1999/xlink');
    $xpc->registerNs('txt', 'http://canadiana.ca/schema/2012/xsd/txtmap');
    $xpc->registerNs('issue', 'http://canadiana.ca/schema/2012/xsd/issueinfo');
    $xpc->registerNs('dc', 'http://purl.org/dc/elements/1.1/');
    $xpc->registerNs('marc', 'http://www.loc.gov/MARC21/slim');

    my @mets = $xpc->findnodes('mets:mets');
    if (!@mets) {
        die "XML file is not METS\n";
    }
    my $mets = @mets[0];

    # Find the master file group, if it exists
    my @masterFileGrp = $xpc->findnodes('mets:fileSec/mets:fileGrp[@USE="master"]',$mets);
    if (scalar(@masterFileGrp) > 1) {
        die "More than one USE=master fileGrp in METS!\n"
    }
    my $masterFileGrp = @masterFileGrp[0];

    # Find the physical structMap (may be other types)
    my @structdiv = $xpc->findnodes('mets:mets/mets:structMap[@TYPE="physical"]/mets:div');
    if (! @structdiv) {
        die "structMap not found in METS\n";
    }
    if (scalar(@structdiv) >1) {
        die "More than one structMap with TYPE=\"physical\" found in METS\n";
    }
    my $structMap=$structdiv[0];

    # Components are divs within structMap
    my @components = $xpc->findnodes('mets:div',$structMap);

    # Set the item label
    if($self->{job}->{label}) {
        my $itemlabel = $self->aipdata->{label} or
            die "Item label update requested, but item label missing\n";

        my $oldlabel=$structMap->getAttribute('LABEL');
        $structMap->setAttribute('LABEL', $itemlabel);

        $self->log->info($self->aip.": Changed item label from '$oldlabel' to '$itemlabel'");
    }

    # Set component labels
    if($self->{job}->{clabel}) {
        my $labels = $self->WIP->wipmeta->get_attachment($self->aip,'labels.json');
        if (!$labels) {
            die "Component label modification requested, but couldn't retrieve labels.json attachment\n";
        }
        my @labels = @{decode_json $labels}; # will croak if not JSON, which is fine
        if (scalar(@components) != scalar(@labels)) {
            die "labels.json count of ".scalar(@labels)." doesn't match METS component count of ".scalar(@components)."\n";
        }
        for (my $i = 0; $i < @components; $i++) {
            $components[$i]->setAttribute('LABEL', $labels[$i]);
        }
        $self->log->info($self->aip.": Changed component labels");
    } 


    # Set item dmdSec
    if($self->{job}->{dmdsec}) {
        my $dmdattach=$self->WIP->wipmeta->get_attachment($self->aip,'dmd.xml');
        if (!$dmdattach) {
            die "Item dmdSec modification requested, but couldn't retrieve dmd.xml attachment\n";
        }
        my $dmd_doc= XML::LibXML->load_xml(string => $dmdattach);

        my $dmdid=$structMap->getAttribute('DMDID');
        my $mdtype = add_dmdsec($doc,$mets,$dmd_doc, $dmdid,1);
        $self->check_item_dmdsec($mdtype);
        $structMap->setAttribute('TYPE', $self->myconfig->{type});
        $self->log->info($self->aip.": updated dmdSec(".$mdtype.") for TYPE=\"".$self->myconfig->{type}."\"");
    }

    # Upload to Couch
    $self->WIP->wipmeta->put_attachment($self->aip, {
        content => $doc->toString(1),
        filename => "metadata.xml",
        type => "application/xml",
                                         });

}

sub check_item_dmdsec {
    my($self,$mdtype) = @_;

    # Verify mdtype appropriate for this item, and type valid
    switch($self->myconfig->{type}) {
        case "series" {
            if ($mdtype ne 'MARC' && $mdtype ne 'DC') {
                die "dmdSec type=$mdtype for series must be 'MARC' or 'DC'\n";
            }
        }
        case "issue" {
            if ($mdtype ne 'issueinfo') {
                die "dmdSec type=$mdtype for issues must be 'issueinfo'\n";
            }
        }
        case "document" {
            if ($mdtype ne 'MARC' && $mdtype ne 'DC') {
                die "dmdSec type=$mdtype for series must be 'MARC' or 'DC'\n";
            }
        }
        else {
            die "Unknown type in configuration\n";
        }
    }
}

sub get_id {
    my($self,$type) = @_;

    my $append='';
    if (exists $self->{id}->{$type}) {
        $append=".".(++($self->{id}->{$type}));
    } else {
        $self->{id}->{$type}=0;
    }
#Do we want the objid in there?    return $type.".".$self->objid.$append;
    return $type.$append;
}

1;
