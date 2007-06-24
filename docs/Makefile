######################################################################
## Different doc packages need different things. Some have to 
## import the source, others just need to parse it. The hacks in
## this file attempt to make this happen for some of the common doc
## systems.
######################################################################
export PYTHON=python
export PRODUCT_DIR=${CURDIR}/../
export PYTHONPATH=`${PYTHON} ./getbuildpath.py`

# If you have epydoc installed feel free
#DOC_TARGETS += epydoc_pdf
DOC_TARGETS += pydoc

all: doc

epydoc_html:
	@epydoc -qq --html -o ${PRODUCT_DIR}/docs ${PYTHONPATH}/${PROJECTNAME}

epydoc_pdf:
	@epydoc -qq --pdf -o ${PRODUCT_DIR}/docs ${PYTHONPATH}/${PROJECTNAME}

pydoc:
	@${PYTHON} ./writedocs.py ${PRODUCT_DIR}


doc: ${DOC_TARGETS}

clean: 
	@find . -name "*.pyc" -exec rm {} \;
	@find . -name "*~" -exec rm {} \;
	@find . -name "${PROJECTNAME}*.html" -exec rm {} \;
	@rm -rf index.html epydoc.css public private
	@rm -rf api.* *.aux *.tex



