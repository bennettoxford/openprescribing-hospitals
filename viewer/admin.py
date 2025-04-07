from django.contrib import admin
from django.core.management import call_command
from django.contrib import messages
from django.urls import path
from django.shortcuts import redirect
from .models import (
    VTM,
    VMP,
    Ingredient,
    Organisation,
    Dose,
    IngredientQuantity,
    Measure,
    MeasureTag,
    WHORoute,
    ContentCache,
)


@admin.register(VTM)
class VTMAdmin(admin.ModelAdmin):
    list_display = ("vtm", "name")
    search_fields = ("vtm", "name")


@admin.register(VMP)
class VMPAdmin(admin.ModelAdmin):
    list_display = ("code", "name", "vtm")
    search_fields = ("code", "name")
    list_filter = ("vtm",)


@admin.register(Ingredient)
class IngredientAdmin(admin.ModelAdmin):
    list_display = ("code", "name")
    search_fields = ("code", "name")


@admin.register(Organisation)
class OrganisationAdmin(admin.ModelAdmin):
    list_display = ("ods_code", "ods_name", "region", "successor")
    search_fields = ("ods_code", "ods_name", "region")
    list_filter = ("region",)


@admin.register(Dose)
class DoseAdmin(admin.ModelAdmin):
    list_display = ('vmp', 'organisation', 'get_latest_quantity', 'get_latest_unit')
    list_filter = ('vmp', 'organisation')
    search_fields = ('vmp__name', 'organisation__ods_name')

    def get_latest_quantity(self, obj):
        if obj.data and len(obj.data) > 0:
            return obj.data[-1][1]
        return None
    get_latest_quantity.short_description = 'Latest Quantity'

    def get_latest_unit(self, obj):
        if obj.data and len(obj.data) > 0:
            return obj.data[-1][2]
        return None
    get_latest_unit.short_description = 'Latest Unit'


@admin.register(IngredientQuantity)
class IngredientQuantityAdmin(admin.ModelAdmin):
    list_display = ('ingredient', 'vmp', 'organisation', 'get_latest_quantity', 'get_latest_unit')
    list_filter = ('ingredient', 'vmp', 'organisation')
    search_fields = ('ingredient__name', 'vmp__name', 'organisation__ods_name')

    def get_latest_quantity(self, obj):
        if obj.data and len(obj.data) > 0:
            return obj.data[-1][1]
        return None
    get_latest_quantity.short_description = 'Latest Quantity'

    def get_latest_unit(self, obj):
        if obj.data and len(obj.data) > 0:
            return obj.data[-1][2]
        return None
    get_latest_unit.short_description = 'Latest Unit'


@admin.register(Measure)
class MeasureAdmin(admin.ModelAdmin):
    list_display = ("name", "slug", "draft")
    search_fields = ("name", "slug")
    list_filter = ("draft", "tags")
    actions = ['import_measure', 'get_measure_vmps', 'compute_measure']
    
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('import-all-measures/', self.admin_site.admin_view(self.import_all_measures_view), 
                 name='import-all-measures'),
        ]
        return custom_urls + urls
    
    def _execute_command_with_output_capture(self, command_name, *args):
        """Helper method to execute a command and capture its output."""
        from io import StringIO
        import sys
        import traceback

        stdout_backup, stderr_backup = sys.stdout, sys.stderr
        stdout_capture, stderr_capture = StringIO(), StringIO()
        sys.stdout, sys.stderr = stdout_capture, stderr_capture
        
        result = {
            'success': False,
            'stdout': '',
            'stderr': '',
            'exception': None,
            'traceback': None
        }
        
        try:
            call_command(command_name, *args)
            result['stdout'] = stdout_capture.getvalue()
            result['stderr'] = stderr_capture.getvalue()
            result['success'] = True
        except Exception as e:
            result['exception'] = e
            result['traceback'] = traceback.format_exc()
            result['success'] = False
        finally:
            sys.stdout, sys.stderr = stdout_backup, stderr_backup
            
        return result
    
    def _process_command_result(self, request, result, context='', level_override=None):
        """Process command execution results and show appropriate messages."""
        if not result['success']:
            self.message_user(
                request,
                f"Error {context}: {str(result['exception'])}\n\nDetails: {result['traceback']}",
                level=messages.ERROR
            )
            return False
            
        if result['stderr']:
            self.message_user(
                request,
                f"Errors occurred {context}: {result['stderr']}",
                level=messages.ERROR
            )
            return False
            
        stdout = result['stdout']
        
        if "ERROR" in stdout:
            error_lines = [line for line in stdout.split('\n') if "ERROR" in line]
            error_message = "\n".join(error_lines)
            self.message_user(
                request,
                f"Errors {context}: {error_message}",
                level=messages.ERROR
            )
            return False
            
        if "Invalid measure definition" in stdout or "tags do not exist" in stdout:
            validation_lines = [line for line in stdout.split('\n') 
                               if "Invalid measure definition" in line or "tags do not exist" in line]
            validation_message = "\n".join(validation_lines)
            self.message_user(
                request,
                f"Validation errors {context}: {validation_message}",
                level=messages.ERROR
            )
            return False
            
        if "WARNING" in stdout:
            warning_lines = [line for line in stdout.split('\n') if "WARNING" in line]
            warning_message = "\n".join(warning_lines)
            self.message_user(
                request,
                f"Warnings {context}: {warning_message}",
                level=messages.WARNING
            )            
        return True

    def import_all_measures_view(self, request):
        result = self._execute_command_with_output_capture('import_measures')
        success = self._process_command_result(request, result, context='during import')
        
        if success and not "WARNING" in result['stdout']:
            self.message_user(
                request,
                "Successfully imported all measures from YAML files",
                level=messages.SUCCESS
            )
            
        return redirect('..')
    
    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context['show_import_all_button'] = True
        return super().changelist_view(request, extra_context=extra_context)
    
    def import_measure(self, request, queryset):
        success_count = 0
        error_count = 0
        
        for measure in queryset:
            result = self._execute_command_with_output_capture('import_measures', measure.slug)
            context = f"importing measure {measure.slug}"
            success = self._process_command_result(request, result, context=context)
            
            if success:
                success_count += 1
            else:
                error_count += 1
        
        if success_count:
            self.message_user(
                request, 
                f"Successfully imported {success_count} measure(s)", 
                level=messages.SUCCESS
            )
    import_measure.short_description = "Import selected measures from YAML"
    
    def get_measure_vmps(self, request, queryset):
        success_count = 0
        error_count = 0
        
        for measure in queryset:
            try:
                call_command('get_measure_vmps', measure.slug)
                success_count += 1
            except Exception as e:
                error_count += 1
                self.message_user(
                    request, 
                    f"Error getting VMPs for measure {measure.slug}: {str(e)}", 
                    level=messages.ERROR
                )
        
        if success_count:
            self.message_user(
                request, 
                f"Successfully retrieved VMPs for {success_count} measure(s)", 
                level=messages.SUCCESS
            )
    get_measure_vmps.short_description = "Get VMPs for selected measures"
    
    def compute_measure(self, request, queryset):
        success_count = 0
        error_count = 0
        
        for measure in queryset:
            try:
                call_command('compute_measures', measure.slug)
                success_count += 1
            except Exception as e:
                error_count += 1
                self.message_user(
                    request, 
                    f"Error computing measure {measure.slug}: {str(e)}", 
                    level=messages.ERROR
                )
        
        if success_count:
            self.message_user(
                request, 
                f"Successfully computed {success_count} measure(s)", 
                level=messages.SUCCESS
            )
    compute_measure.short_description = "Compute selected measures"


@admin.register(MeasureTag)
class MeasureTagAdmin(admin.ModelAdmin):
    list_display = ("name", "colour")
    search_fields = ("name", "colour")


@admin.register(WHORoute)
class WHORouteAdmin(admin.ModelAdmin):
    list_display = ("code", "name")
    search_fields = ("code", "name")


class ContentCacheAdmin(admin.ModelAdmin):
    """Admin interface for managing content cache"""
    list_display = ('last_updated',)
    
    def has_add_permission(self, request):
        return False
    
    def has_delete_permission(self, request, obj=None):
        return False
    
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('', self.admin_site.admin_view(self.custom_changelist_view), name='viewer_contentcache_changelist'),
            path('refresh-content-cache/', 
                 self.admin_site.admin_view(self.refresh_content_cache_view),
                 name='refresh-content-cache'),
            path('clear-content-cache/', 
                 self.admin_site.admin_view(self.clear_content_cache_view),
                 name='clear-content-cache'),
        ]
        return custom_urls + urls
    
    def refresh_content_cache_view(self, request):
        try:
            call_command('refresh_content_cache')
            self.message_user(
                request,
                "Successfully refreshed content cache",
                level=messages.SUCCESS
            )
        except Exception as e:
            self.message_user(
                request,
                f"Error refreshing content cache: {str(e)}",
                level=messages.ERROR
            )
        return redirect('..')
    
    def clear_content_cache_view(self, request):
        try:
            from django.core.cache import cache
            cache.delete('bennett_blog_data')
            cache.delete('bennett_papers_data')
            self.message_user(
                request,
                "Successfully cleared content cache",
                level=messages.SUCCESS
            )
        except Exception as e:
            self.message_user(
                request,
                f"Error clearing content cache: {str(e)}",
                level=messages.ERROR
            )
        return redirect('..')
    
    def custom_changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context['title'] = "Content Cache"
        
        self.message_user(
            request, 
            "Use the 'Refresh Content Cache' button to update blog posts and papers from Bennett Institute.",
            level=messages.INFO
        )
        
        from django.template.response import TemplateResponse
        context = {
            'opts': self.model._meta,
            'app_label': self.model._meta.app_label,
            'title': 'Content Cache',
            'refresh_url': 'refresh-content-cache/',
            'clear_url': 'clear-content-cache/',
            **self.admin_site.each_context(request),
            **(extra_context or {}),
        }
        return TemplateResponse(request, 'admin/viewer/contentcache/change_list.html', context)


admin.site.register(ContentCache, ContentCacheAdmin)